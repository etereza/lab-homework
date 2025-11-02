from airflow.utils.email import send_email

def email_on_failure(context):
    ti = context["task_instance"]
    subject = f"[Airflow] Task failed: {ti.task_id} in DAG {ti.dag_id}"
    body = f'''
    <h3>Airflow Task Failure</h3>
    <b>DAG:</b> {ti.dag_id}<br>
    <b>Task:</b> {ti.task_id}<br>
    <b>Execution date:</b> {context.get("ts")}<br>
    <b>Try number:</b> {ti.try_number}<br>
    <a href="{ti.log_url}">Open Logs</a>
    '''
    send_email(to=["you@example.com"], subject=subject, html_content=body)
