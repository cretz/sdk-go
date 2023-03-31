package workflowfuzzer

import (
	"fmt"

	"go.temporal.io/api/command/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/temporal"
)

var ErrSimulated = temporal.NewApplicationError("Simulated error", "Simulated")

type Handler interface {
	OnStart()

	// Delegates to the different command handlers
	OnCommand(*command.Command)

	// Command handlers
	OnCommandScheduleActivity(*command.ScheduleActivityTaskCommandAttributes)
	OnCommandRequestCancelActivity(*command.RequestCancelActivityTaskCommandAttributes)
}

type DefaultHandler struct{ Run *FuzzRun }

var _ Handler = &DefaultHandler{}

func NewDefaultHandler(run *FuzzRun) *DefaultHandler { return &DefaultHandler{run} }

func (d *DefaultHandler) OnStart() {
	// Just send the started event. Known signals may be sent at any point during
	// task building.
	d.Run.AddHistoryEvent(&history.HistoryEvent{
		Attributes: &history.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: d.Run.StartedEvent,
		},
	})
}

func (d *DefaultHandler) OnCommand(cmd *command.Command) {
	switch cmd.CommandType {
	case enums.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK:
		d.Run.Handler.OnCommandScheduleActivity(cmd.GetScheduleActivityTaskCommandAttributes())
	case enums.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK:
		d.Run.Handler.OnCommandRequestCancelActivity(cmd.GetRequestCancelActivityTaskCommandAttributes())
	}
}

func (d *DefaultHandler) OnCommandScheduleActivity(cmd *command.ScheduleActivityTaskCommandAttributes) {
	// Always schedule immediately
	scheduledEvent := d.Run.AddActivityScheduled(cmd)

	// Maybe start
	d.Run.AddMaybeInFuture("", func() {
		d.Run.AddActivityStarted(cmd)
		// Maybe complete
		d.Run.AddMaybeInFuture(
			fmt.Sprintf("activity-complete-%v", scheduledEvent.EventId),
			func() {
				completer := d.Run.ActivityCompleters[cmd.ActivityType.Name]
				if completer == nil {
					completer = DefaultActivityCompleter
				}
				completer(d.Run, cmd)
			},
		)
	})
}

func (d *DefaultHandler) OnCommandRequestCancelActivity(cmd *command.RequestCancelActivityTaskCommandAttributes) {
	// Add cancel requested immediately
	d.Run.AddActivityCancelRequested(cmd)

	// Maybe actually cancel (mutually exclusive with other complete)
	d.Run.AddMaybeInFuture(
		fmt.Sprintf("activity-complete-%v", cmd.ScheduledEventId),
		func() {
			activityType := d.Run.EventByID(cmd.ScheduledEventId).GetActivityTaskScheduledEventAttributes().ActivityType.Name
			canceller := d.Run.ActivityCancellers[activityType]
			if canceller == nil {
				canceller = DefaultActivityCanceller
			}
			canceller(d.Run, cmd)
		},
	)
}

func DefaultActivityCompleter(run *FuzzRun, cmd *command.ScheduleActivityTaskCommandAttributes) {
	switch {
	case run.PermBool():
		run.AddActivityFailed(cmd, ErrSimulated)
	case cmd.ScheduleToCloseTimeout != nil && run.PermBool():
		run.AddActivityTimedOut(cmd, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, nil))
	case cmd.ScheduleToStartTimeout != nil && run.PermBool():
		run.AddActivityTimedOut(cmd, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_SCHEDULE_TO_START, nil))
	case cmd.StartToCloseTimeout != nil && run.PermBool():
		run.AddActivityTimedOut(cmd, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil))
	case cmd.HeartbeatTimeout != nil && run.PermBool():
		run.AddActivityTimedOut(cmd, temporal.NewHeartbeatTimeoutError())
	default:
		run.AddActivitySucceeded(cmd, nil)
	}
}

func DefaultActivityCanceller(run *FuzzRun, cmd *command.RequestCancelActivityTaskCommandAttributes) {
	run.AddActivityCancelled(cmd)
}
