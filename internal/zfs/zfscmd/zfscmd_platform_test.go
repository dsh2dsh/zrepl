package zfscmd

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBin = "./zfscmd_platform_test.sh"

func TestCmdStderrBehaviorOutput(t *testing.T) {
	stdout, err := exec.Command(testBin, "0").Output()
	require.NoError(t, err)
	require.Equal(t, []byte("to stdout\n"), stdout)

	stdout, err = exec.Command(testBin, "1").Output()
	assert.Equal(t, []byte("to stdout\n"), stdout)
	require.Error(t, err)
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	require.Equal(t, ee.Stderr, []byte("to stderr\n"))
}

func TestCmdStderrBehaviorCombinedOutput(t *testing.T) {
	stdio, err := exec.Command(testBin, "0").CombinedOutput()
	require.NoError(t, err)
	require.Equal(t, "to stderr\nto stdout\n", string(stdio))

	stdio, err = exec.Command(testBin, "1").CombinedOutput()
	require.Equal(t, "to stderr\nto stdout\n", string(stdio))
	require.Error(t, err)
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	require.Empty(t, ee.Stderr) // !!!! maybe not what one would expect
}

func TestCmdStderrBehaviorStdoutPipe(t *testing.T) {
	cmd := exec.Command(testBin, "1")
	stdoutPipe, err := cmd.StdoutPipe()
	require.NoError(t, err)
	err = cmd.Start()
	require.NoError(t, err)
	defer cmd.Wait()
	var stdout bytes.Buffer
	_, err = io.Copy(&stdout, stdoutPipe)
	require.NoError(t, err)
	require.Equal(t, "to stdout\n", stdout.String())

	err = cmd.Wait()
	require.Error(t, err)
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	// !!!!! probably not what one would expect if we only redirect stdout
	require.Empty(t, ee.Stderr)
}

func TestCmdProcessState(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", "echo running; sleep 3600")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	err = cmd.Start()
	require.NoError(t, err)

	r := bufio.NewReader(stdout)
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "running\n", line)

	// we know it's running and sleeping
	cancel()
	err = cmd.Wait()
	t.Logf("wait err %T\n%s", err, err)
	require.Error(t, err)
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	require.NotNil(t, ee.ProcessState)
	require.Contains(t, ee.Error(), "killed")
}

func TestSigpipe(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := CommandContext(ctx, "sh", "-c", "sleep 5; echo invalid input; exit 23")
	r, w, err := os.Pipe()
	require.NoError(t, err)
	var output bytes.Buffer
	cmd.setStdio(Stdio{
		Stdin:  r,
		Stdout: &output,
		Stderr: &output,
	})
	err = cmd.Start()
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)

	// the script doesn't read stdin, but this input is almost certainly smaller than the pipe buffer
	const LargerThanPipeBuffer = 1 << 21
	_, err = io.Copy(w, bytes.NewBuffer(bytes.Repeat([]byte("i"), LargerThanPipeBuffer)))
	// => io.Copy is going to block because the pipe buffer is full and the
	//    script is not reading from it
	// => the script is going to exit after 5s
	// => we should expect a broken pipe error from the copier's perspective
	t.Logf("copy err = %T: %s", err, err)
	require.Error(t, err)
	require.Contains(t, err.Error(), "broken pipe")

	err = cmd.Wait()
	require.ErrorContains(t, err, "exit status 23")
}

func TestCmd_Pipe(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name         string
		cmd          []string
		pipe         [][]string
		pipeFrom     bool
		wantCmdStr   string
		startErr     bool
		waitErr      bool
		assertStdout func(t *testing.T, b []byte)
	}{
		{
			name:       "no error",
			cmd:        []string{"echo", "foobar"},
			pipe:       [][]string{{"tr", "a-z", "A-Z"}},
			wantCmdStr: "echo foobar | tr a-z A-Z",
			assertStdout: func(t *testing.T, b []byte) {
				assert.Equal(t, "FOOBAR", strings.TrimSpace(string(b)))
			},
		},
		{
			name:       "from pipe",
			cmd:        []string{"cat"},
			pipe:       [][]string{{"echo", "foobar"}},
			pipeFrom:   true,
			wantCmdStr: "echo foobar | cat",
		},
		{
			name:       "cmd error",
			cmd:        []string{"false"},
			pipe:       [][]string{{"tr", "a-z", "A-Z"}},
			wantCmdStr: "false | tr a-z A-Z",
			waitErr:    true,
		},
		{
			name:       "pipe error",
			cmd:        []string{"echo", "foobar"},
			pipe:       [][]string{{"false"}},
			wantCmdStr: "echo foobar | false",
			waitErr:    true,
		},
		{
			name:       "cmd not found",
			cmd:        []string{"/this-command-doesnt-exists"},
			pipe:       [][]string{{"true"}},
			wantCmdStr: "/this-command-doesnt-exists | true",
			startErr:   true,
		},
		{
			name:       "pipe not found",
			cmd:        []string{"echo", "foobar"},
			pipe:       [][]string{{"/this-command-doesnt-exists"}},
			wantCmdStr: "echo foobar | /this-command-doesnt-exists",
			startErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := New(ctx).WithPipeLen(len(tt.pipe)).
				WithCommand(tt.cmd[0], tt.cmd[1:])

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			var pipeReader io.Reader
			var err error

			if tt.pipeFrom {
				require.NoError(t, cmd.PipeFrom(tt.pipe, nil, &stdout, &stderr))
				pipeReader = &stdout
			} else {
				pipeReader, err = cmd.PipeTo(tt.pipe, nil, &stderr)
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantCmdStr, cmd.String())

			if tt.startErr {
				require.Error(t, cmd.startPipe())
				require.NoError(t, cmd.waitPipe())
				return
			}
			require.NoError(t, cmd.startPipe())

			b, err := io.ReadAll(pipeReader)
			require.NoError(t, err)

			if tt.waitErr {
				require.Error(t, cmd.waitPipe())
			} else {
				require.NoError(t, cmd.waitPipe())
			}
			assert.Empty(t, stderr.String())

			if tt.assertStdout != nil {
				tt.assertStdout(t, b)
			}
		})
	}
}
