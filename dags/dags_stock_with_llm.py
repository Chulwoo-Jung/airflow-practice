from practical.get_stock_df import get_m7_stock_df
from airflow.sdk import DAG, task
import pendulum

with DAG(
    dag_id='dags_stock_with_llm',
    schedule_interval='* 8 * * 5',
    start_date=pendulum.datetime(2025, 6, 25, tz='Europe/Berlin'),
    catchup=False,
    tags=['stock', 'llm']
) as dag:

    @task(task_id='get_m7_stock_df', retries=3)
    def get_m7_stock_df():
        return get_m7_stock_df()
    
    @task(task_id='get_output_from_llm', retries=3)
    def get_output_from_llm(**kwargs):
        stock_df = kwargs['ti'].xcom_pull(task_ids='get_m7_stock_df')
        
        import langchain_openai
        from langchain_core.output_parsers import StrOutputParser

        llm = langchain_openai.ChatOpenAI(model="gpt-4o-mini", temperature=0.2)
        parser = StrOutputParser()
        
        prompt = ''' 
        {stock_df}는 5일간 미국 nasdaq의 magnificant 7의 주식표야.
        저걸 기반으로 7일에 한번 주식에 관한 메일을 보내는 글을 만들어줘.
        각 회사마다 꼭 봐야했던 주요한 issuse들을 같이 넣고, 
        마지막에는 한주간 꼭 챙겨봐야할 뉴스 소식(중복되지 않게, 주요 경제소식)을 넣을거야.
        모든 글을 최종적으로 *꼭 html 형식*으로 만들어줘.
        예시)
        
        {stock_df}  (우선 표를 보여줘 그리고 인사말과 함께 이번주는 어떤 소식을 주목해야할지 먼저 알려줘)

        1. Apple
        {애플의 주가 형성의 이유}
        - {주가와 관련한 뉴스 1}
        - {주가와 관련한 뉴스 2}
        - {주가와 관련한 뉴스 3}

        2. 

        3.

        {반복}

        ** 이번주 주요 뉴스
        {주요 뉴스 5개 정도 나열}
        '''

        llm_chain = prompt | llm | parser
        response = llm_chain.invoke(stock_df)
        return response
    
    @task(task_id='send_email', retries=3)
    def send_email(**kwargs):
        from airflow.providers.smtp.operators.smtp import EmailOperator        
        response = kwargs['ti'].xcom_pull(key='return_value', task_ids='get_output_from_llm')

        email_list = '{{var.value.email_target}}'.split(',')
        email_list = [email.strip() for email in email_list]

        EmailOperator(
            task_id='send_email',
            conn_id='conn_smtp_gmail',
            to=email_list,
            subject='🔥 M7 Titans: This Week\'s Market Movers | {{ data_interval_end.in_timezone("Europe/Berlin") | ds }}',
            html_content=response
        )

    get_m7_stock_df() >> get_output_from_llm() >> send_email()