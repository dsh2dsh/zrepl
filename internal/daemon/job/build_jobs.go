package job

import (
	"fmt"

	"github.com/dsh2dsh/zrepl/internal/config"
)

func JobsFromConfig(c *config.Config) ([]Job, *Connecter, error) {
	jobs := make([]Job, len(c.Jobs))
	connecter := NewConnecter(c.Keys).WithTimeout(c.Global.RpcTimeout)

	for i := range c.Jobs {
		j, err := buildJob(&c.Global, c.Jobs[i], connecter)
		if err != nil {
			return nil, nil, err
		}
		if j == nil || j.Name() == "" {
			panic(fmt.Sprintf(
				"implementation error: job builder returned nil job type %T",
				c.Jobs[i].Ret))
		}
		jobs[i] = j
	}

	if err := connecter.Validate(); err != nil {
		return nil, nil, fmt.Errorf("job inconsistency: %w", err)
	}
	return jobs, connecter, nil
}

func buildJob(c *config.Global, in config.JobEnum, connecter *Connecter,
) (j Job, err error) {
	cannotBuildJob := func(err error, name string) (Job, error) {
		return nil, fmt.Errorf("cannot build job %q: %w", name, err)
	}

	// FIXME prettify this
	switch v := in.Ret.(type) {
	case *config.SinkJob:
		j, err = passiveSideFromConfig(c, &v.PassiveJob, v, connecter)
		if err != nil {
			return cannotBuildJob(err, v.Name)
		}
	case *config.SourceJob:
		j, err = passiveSideFromConfig(c, &v.PassiveJob, v, connecter)
		if err != nil {
			return cannotBuildJob(err, v.Name)
		}
	case *config.SnapJob:
		j, err = snapJobFromConfig(c, v)
		if err != nil {
			return cannotBuildJob(err, v.Name)
		}
	case *config.PushJob:
		j, err = activeSide(c, &v.ActiveJob, v, connecter)
		if err != nil {
			return cannotBuildJob(err, v.Name)
		}
	case *config.PullJob:
		j, err = activeSide(c, &v.ActiveJob, v, connecter)
		if err != nil {
			return cannotBuildJob(err, v.Name)
		}
	default:
		panic(fmt.Sprintf("implementation error: unknown job type %T", v))
	}
	return j, nil
}
