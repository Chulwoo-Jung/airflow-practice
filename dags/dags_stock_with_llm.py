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
        from airflow.models import Variable
        openai_api_key = Variable.get("openai_api_key")
        
        llm = langchain_openai.ChatOpenAI(
            api_key=openai_api_key,
            model="o4-mini", 
        )
        parser = StrOutputParser()
        
        # Convert DataFrame to HTML table
        stock_df_html = stock_df.to_html(index=False, classes='table table-striped')
        
        prompt = PromptTemplate(
            input_variables=['stock_df_html'],
            template=''' 
        {stock_df_html}
        
        이 데이터를 기반으로 주식에 관한 메일을 보내는 글을 만들어주세요.
        우선 인사말과 함께 이번주는 어떤 소식을 주목해야할지 먼저 알려주세요. 그리고 모든 데이터를 보기좋게 가공해서 보여주세요.
        각 회사마다 꼭 봐야했던 주요한 이슈들을 함께 넣고, 
        마지막에는 한주간 꼭 챙겨봐야할 뉴스 소식(한국 뉴스로 중복되지 않게, 주요 경제소식)을 넣어주세요.
        각각의 뉴스 소식에 3줄 정도의 설명도 함께 넣어주세요.
        모든 글을 최종적으로 *꼭 html 형식*으로 만들어주세요. 꼭 *독자가 읽기 재밌도록 쓰세요.*
        마치 GPT가 작성하지 않은 것 처럼 쓰세요.
        
        예시 형식:
        
        <h2>가장 주목할 뉴스 1개를 기반으로 친근한 제목을 만들어주세요. 이모티콘을 사용해도 됩니다.( ex = 끝나지 않는 관세 전쟁 ) </h2>
        <p>인사말과 함께 이번주는 어떤 소식을 주목해야할지 먼저 알려주세요.(이번주는 유독 날씨가 추웠다, 미국 주식시장은 좋지 않았다, 중동 정세 불안으로 인해 주가가 낮아졌다, 등등)</p>
    
        
        <h3>1. Apple (AAPL)</h3>
        (여기에 {stock_df_html}에서 Name과 Ticker를 제외한 애플의 나머지 데이터를 보기좋게 가공해서 보여주세요)
        <p>애플의 주가 형성의 이유</p>
        <ul>
        <li>주가와 관련한 뉴스 1</li>
        <li>주가와 관련한 뉴스 2</li>
        <li>주가와 관련한 뉴스 3</li>
        </ul>
        
        <h3>2. Microsoft (MSFT)</h3>
        (여기에 {stock_df_html}에서 Name과 Ticker를 제외한 마이크로소프트의 나머지 데이터를 보기좋게 가공해서 보여주세요)
        <p>마이크로소프트의 주가 형성의 이유</p>
        <ul>
        <li>주가와 관련한 뉴스 1</li>
        <li>주가와 관련한 뉴스 2</li>
        <li>주가와 관련한 뉴스 3</li>
        </ul>
        
        <h3>3. Google (GOOGL)</h3>
        (여기에 {stock_df_html}에서 Name과 Ticker를 제외한 구글의 나머지 데이터를 보기좋게 가공해서 보여주세요)
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
        (여기서 추가로 생성하지 말고 끝내주세요)
        '''
        )

        chain = prompt | llm | parser
        response = chain.invoke({"stock_df_html": stock_df_html})
        return response
    
    @task(task_id='send_email')
    def send_email(**kwargs):
        from airflow.providers.smtp.operators.smtp import EmailOperator        
        from airflow.models import Variable
        import pendulum
        
        response = kwargs['ti'].xcom_pull(key='return_value', task_ids='get_output_from_llm')

        email_target = Variable.get("email_target")
        email_list = email_target.split(',')
        email_list = [email.strip() for email in email_list]

        formatted_date = pendulum.now('Europe/Berlin').strftime('%Y-%m-%d')
        subject = f"🔥 M7 Titans: This Week's Market Movers | {formatted_date} | Weekly Report"
        
        email_op = EmailOperator(
            task_id='send_email',
            conn_id='conn_smtp_gmail',
            to=email_list,
            subject=subject,
            html_content=response
        )
        email_op.execute(context=kwargs)

    get_m7_stock_df() >> get_output_from_llm() >> send_email()