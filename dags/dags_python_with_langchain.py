from airflow.sdk import DAG, task
import pendulum
from langchain_openai import ChatOpenAI

with DAG(
    dag_id='dags_python_with_langchain',
    schedule=None,
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    
    
    @task(task_id='generate_content')
    def generate_content(**kwargs):
        """Content generation"""
        llm = ChatOpenAI(
            api_key='{{var.value.openai_api_key}}',
            model="gpt-4o-mini",
            temperature=0.8
        )
        
        prompt = """
        Write a short explanation about the following topic:
        Topic: The advantages and use cases of Apache Airflow
        
        Explain in 3-4 sentences.
        """
        
        response = llm.invoke(prompt)
        print(f"Generated content: {response.content}")
        return response.content
    
    @task(task_id='analyze_sentiment')
    def analyze_sentiment(**kwargs):
        """Sentiment analysis"""
        # Get the result of the previous task
        ti = kwargs['ti']
        content = ti.xcom_pull(task_ids='generate_content')
        
        # Analyze with lower temperature
        llm = ChatOpenAI(
            api_key='{{var.value.openai_api_key}}',
            model="gpt-4o-mini",
            temperature=0.2
        )
        
        analysis_prompt = f"""
        Analyze the sentiment and tone of the following text:
        
        Text: {content}
        
        Summarize the analysis result in a few sentences.
        """
        
        response = llm.invoke(analysis_prompt)
        print(f"Sentiment analysis result: {response.content}")
    
    # Task execution order
    generate_content() >> analyze_sentiment() 