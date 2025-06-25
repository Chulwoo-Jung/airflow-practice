from practical.get_stock_df import get_m7_stock_df
from airflow.sdk import DAG, task
import pendulum

with DAG(
    dag_id='dags_stock_with_llm',
    schedule='* 8 * * 5',
    start_date=pendulum.datetime(2025, 6, 25, tz='Europe/Berlin'),
    catchup=False,
    tags=['stock', 'llm']
) as dag:

    @task(task_id='get_m7_stock_df')
    def get_m7_stock_df():
        from practical.get_stock_df import get_m7_stock_df
        df = get_m7_stock_df()
        # Convert DataFrame to JSON
        return df.to_json(orient='records', date_format='iso')
    
    @task(task_id='get_output_from_llm')
    def get_output_from_llm(**kwargs):
        import json
        import pandas as pd
        
        # Convert JSON to DataFrame
        stock_df_json = kwargs['ti'].xcom_pull(task_ids='get_m7_stock_df')
        stock_df = pd.read_json(stock_df_json, orient='records')
        
        import langchain_openai
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import PromptTemplate
        from langchain.chains import LLMChain
        from airflow.models import Variable
        openai_api_key = Variable.get("openai_api_key")
        
        llm = langchain_openai.ChatOpenAI(
            api_key=openai_api_key,
            model="gpt-4o-mini", 
            temperature=0.2
        )
        parser = StrOutputParser()
        
        # Convert DataFrame to HTML table
        stock_df_html = stock_df.to_html(index=False, classes='table table-striped')
        
        prompt = PromptTemplate(
            input_variables=['stock_df_html'],
            template=''' 
        {stock_df_html}
        
        이 데이터를 기반으로 주식에 관한 메일을 보내는 글을 만들어주세요.
        우선 {stock_df_html}를 보여주고, 그 다음에 인사말과 함께 이번주는 어떤 소식을 주목해야할지 먼저 알려주세요.
        각 회사마다 꼭 봐야했던 주요한 이슈들을 같이 넣고, 
        마지막에는 한주간 꼭 챙겨봐야할 뉴스 소식(중복되지 않게, 주요 경제소식)을 넣어주세요.
        모든 글을 최종적으로 *꼭 html 형식*으로 만들어주세요.
        
        예시 형식:
        
        <h2>이번주 주목해야 할 소식</h2>
        <p>인사말과 함께 이번주는 어떤 소식을 주목해야할지 먼저 알려주세요.</p>
        
        <h3>1. Apple (AAPL)</h3>
        <p>애플의 주가 형성의 이유</p>
        <ul>
        <li>주가와 관련한 뉴스 1</li>
        <li>주가와 관련한 뉴스 2</li>
        <li>주가와 관련한 뉴스 3</li>
        </ul>
        
        <h3>2. Microsoft (MSFT)</h3>
        <p>마이크로소프트의 주가 형성의 이유</p>
        <ul>
        <li>주가와 관련한 뉴스 1</li>
        <li>주가와 관련한 뉴스 2</li>
        <li>주가와 관련한 뉴스 3</li>
        </ul>
        
        <h3>3. Google (GOOGL)</h3>
        <p>구글의 주가 형성의 이유</p>
        <ul>
        <li>주가와 관련한 뉴스 1</li>
        <li>주가와 관련한 뉴스 2</li>
        <li>주가와 관련한 뉴스 3</li>
        </ul>
        
        ... (반복)
        
        <h2>이번주 주요 뉴스</h2>
        <ul>
        <li>주요 뉴스 1</li>
        <li>주요 뉴스 2</li>
        <li>주요 뉴스 3</li>
        <li>주요 뉴스 4</li>
        <li>주요 뉴스 5</li>
        </ul>
        '''
        )

        llm_chain = LLMChain(llm=llm, prompt=prompt, output_parser=parser)
        response = llm_chain.invoke({"stock_df_html": stock_df_html})
        return response
    
    @task(task_id='send_email')
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