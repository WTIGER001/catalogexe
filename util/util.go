package util

import (
	//	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	docker "github.com/fsouza/go-dockerclient"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"strconv"
	"strings"
)

type Processor struct {
	Name    string   `json:"name"`
	Docker  string   `json:"docker"`
	Cpu     float32  `json:"cpus"`
	Mem     int64    `json:"mem"`
	Input   string   `json:"input"`
	Output  string   `json:"output"`
	Error   string   `json:"error"`
	Volumes []Volume `json:"volumes"`
	Env     []string `json:"env"`
}
type Volume struct {
	HostPath      string `json:"host-path"`
	ContainerPath string `json:"container-path"`
	Mode          string `json:"mode"`
}
type Metadata struct {
	Filename string `json:"filename"`
	Datatype string `json:"datatype"`
	Size     int64  `json:"size"`
	Hash     string `json:"hasha"`
}
type Location struct {
	Url        string `json:"url"`
	Type       string `json:"type"`
	Compressed bool   `json:"compressed"`
	Processing string `json:"processing"`
}
type Message struct {
	Metadata  Metadata   `json:"metadata"`
	Locations []Location `json:"locations"`
}

func RunDocker(taskInfo *mesos.TaskInfo) (string, int, error) {
	fmt.Println("IN DOCKER RUN")

	// Get out the processor
	processorStr := FindLabelValue("processor", taskInfo.GetLabels())
	if processorStr == "" {
		return "", -1, errors.New("Missing Processor")
	}
	fmt.Println("Processor: " + processorStr)
	var p Processor
	if err := json.Unmarshal([]byte(processorStr), &p); err != nil {
		return "", -2, err
	}

	// Get out the message
	messageStr := FindLabelValue("message", taskInfo.GetLabels())
	fmt.Println("MESSAGE: " + messageStr)
	if messageStr == "" {
		return "", -3, errors.New("Missing Message")
	}
	var message Message
	if err := json.Unmarshal([]byte(messageStr), &message); err != nil {
		return "", -4, err
	}

	//
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	var opts docker.CreateContainerOptions
	var cfg docker.Config
	var host docker.HostConfig

	cfg.Image = p.Docker
	cfg.AttachStdout = true
	cfg.AttachStderr = true

	cfg.Memory = p.Mem * 1024000 // Memory is in MB in Processor and Bytes in Docker

	// Add all the task labels as Environment variables
	fmt.Println("SIZE " + strconv.Itoa(taskInfo.GetLabels().Size()))
	for i := 0; i < len(taskInfo.GetLabels().GetLabels()); i++ {
		fmt.Println("getting " + strconv.Itoa(i))
		l := taskInfo.GetLabels().GetLabels()[i]
		cfg.Env = append(cfg.Env, l.GetKey()+"="+l.GetValue())
	}

	// Add all environment variables from the processor
	for _, envRequest := range p.Env {
		cfg.Env = append(cfg.Env, envRequest)
	}

	// Attach the ingest file. The MetaData Parsers expect that
	// the data is mounted under /data/ingest_file
	path := findInFile(&message)
	if path == "" {
		return "", -5, errors.New("No File to Process!")
	}
	cfg.Mounts = append(cfg.Mounts, docker.Mount{
		Name:   "/data/ingest_file",
		Source: path,
		RW:     false,
	})
	host.Binds = append(host.Binds, path+":/data/ingest_file")

	// Attach any volume that is configured
	for _, volRequest := range p.Volumes {

		cfg.Mounts = append(cfg.Mounts, docker.Mount{
			Name:   volRequest.ContainerPath,
			Source: volRequest.HostPath,
			RW:     volRequest.Mode == "RW",
		})

		host.Binds = append(host.Binds, volRequest.HostPath+":"+volRequest.ContainerPath)
	}

	opts.Config = &cfg
	opts.HostConfig = &host
	opts.Name = taskInfo.TaskId.GetValue()

	// Create the container
	container, err := createAndPull(client, &p, opts)

	// Run the Container
	err = client.StartContainer(container.ID, nil)
	if err != nil {
		return "", -7, err
	}

	var b bytes.Buffer

	// Attach to the container
	err = client.AttachToContainer(docker.AttachToContainerOptions{
		Container:    container.ID,
		OutputStream: &b,
		Stdout:       true,
		Logs:         true,
	})

	// Wait for the container to finish
	code, err := client.WaitContainer(container.ID)
	if err != nil {
		panic(err)
	}

	fmt.Println("DATA:  "+b.String(), "code", code)

	return container.ID, code, nil
}

func createAndPull(client *docker.Client, p *Processor, opts docker.CreateContainerOptions) (*docker.Container, error) {
	var container *docker.Container

	container, err := client.CreateContainer(opts)
	if err != nil {
		if err.Error() == "no such image" {
			fmt.Println("Trying to Pull Docker Image ", p.Docker)

			// Pull the images
			errPull := client.PullImage(docker.PullImageOptions{Repository: p.Docker}, docker.AuthConfiguration{})
			if errPull != nil {
				return nil, errPull
			}
			fmt.Println("Pulled Docker Image ", p.Docker)

			fmt.Println("Creating the Container ", p.Docker)
			container, err := client.CreateContainer(opts)
			if err != nil {
				return nil, err
			}
			return container, nil
		}
		return nil, err
	}
	return container, err
}

func findInFile(message *Message) string {
	f := findFile(message, "ingest")
	if f == nil {
		f = findFile(message, "archive")
	}
	if f == nil {
		f = findFile(message, "cache")
	}
	if f == nil {
		f = findFile(message, "")
	}
	if f == nil {
		return ""
	} else {
		return f.Url[7:]
	}
}

func findFile(message *Message, typeOfFile string) *Location {
	for _, loc := range message.Locations {
		fmt.Println("Looking At : " + loc.Url)
		if typeOfFile == "" || loc.Type == typeOfFile {
			if strings.HasPrefix(loc.Url, "file://") {
				return &loc
			}
		}
	}
	return nil
}

func FindLabelValue(key string, labels *mesos.Labels) string {
	for i := 0; i < len(labels.GetLabels()); i++ {
		if key == labels.GetLabels()[i].GetKey() {
			return labels.GetLabels()[i].GetValue()
		}
	}
	return ""
}
