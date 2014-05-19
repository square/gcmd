package gcmd

import (
	"os/exec"
	"bufio"
	"fmt"
	"sync"
	"strings"
)

type StdoutHandlerFunc func(o []byte)
type StderrHandlerFunc func(e []byte)
type ExitHandlerFunc   func(exit int)

type Gcmd struct {
	Timeout   int
	Maxflight int
	command   string
	command_args []string
	nodes     []string
	remaining int
}

func New(nodes []string, command string, command_args ...string) *Gcmd {
	g := new(Gcmd)
	g.nodes = nodes
	g.command = command
	g.command_args = command_args
	return g
}

// Run the command with maxflight number of parallel
// processes and marker __NODE__ replaced with node
// name
func (g *Gcmd) Run() {

	maxflightChan := make(chan string, g.Maxflight)
	var wg sync.WaitGroup

	for g.remaining = len(g.nodes); g.remaining > 0; g.remaining-- {
		fmt.Println("Remaining: ", g.remaining)
		node := g.nodes[len(g.nodes) - g.remaining]
		maxflightChan <- node
		command_args := g.replaceMarker(node, g.command_args)

		// run each process in a goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				<-maxflightChan
			}()

			fmt.Println("Executing: ", g.command, g.remaining, len(g.nodes))
			cmd := exec.Command(g.command, command_args...)

			// setup stdout pipe
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				g.ErrorHandler(node, err)
				return
			}

			// setup stderr pipe
			stderr, err := cmd.StderrPipe()
			if err != nil {
				g.ErrorHandler(node, err)
				return
			}

			// run the command
			if err = cmd.Start(); err != nil {
				return
			}

			// read from stdout/stderr and invoke
			// user supplied handlers
			stdoutScanner := bufio.NewScanner(stdout)
			stderrScanner := bufio.NewScanner(stderr)

			wg.Add(1)
			go func() {
				defer wg.Done()
				for stdoutScanner.Scan() {
					g.StdoutHandler(node, stdoutScanner.Text())
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for stderrScanner.Scan() {
					g.StderrHandler(node, stdoutScanner.Text())
				}
			}()

			if err = cmd.Wait(); err != nil {
				return
			}

		}()
	}
	wg.Wait()
}

// unexported methods

// TODO: make replace marker configurable
func (g *Gcmd) replaceMarker() []string {
	var command_args []string
	for _, arg := range g.command_args {
		command_args = append(command_args,
		strings.Replace(arg, "__NODE__", node, -1))
	}
	return command_args
}
