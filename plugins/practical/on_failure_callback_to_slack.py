from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def on_failure_callback_to_slack(context):
    ti = context.get('ti')
    dag_id = ti.dag_id
    task_id = ti.task_id
    err_msg = context.get('exception')
    batch_date = context.get('data_interval_end').in_timezone('Europe/Berlin')

    slack_hook = SlackWebhookHook(slack_webhook_conn_id='conn_slack_airflow_bot')
    text = "Failure Alert"
    blocks = [  # Using Slack Block Builder to format the message
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"*{dag_id}.{task_id} Failure Alert*"
			}
		},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Batch Time*: {batch_date}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Error Message*: {err_msg}"
                }
            ]
        }
    ]

    slack_hook.send(text=text, blocks=blocks)