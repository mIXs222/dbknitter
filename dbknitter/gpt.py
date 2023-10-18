import requests
import time
import datetime
import openai
import os
import argparse

os.environ['OPENAI_API_KEY'] = "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"
openai.api_key = os.getenv("OPENAI_API_KEY")
MAX_TOKEN=2000

class Utility:
    def __init__(self):
        pass
    
    def get_current_time(self):
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y%m%d_%H:%M:%S")
        return formatted_datetime
util = Utility()


''' 
Example Prompt:

- I have a mysql table named DETAILS and a mongoldb table named INTERESTS.
- DETAILS has the columns NAME, AGE, ADDRESS. INTERESTS has the fields of NAME, INTEREST. INTEREST is a list of strings and can be empty.
- But the user of my data thinks all data is stored in mysql.
- They wrote the following query:
- SELECT DETAILS.AGE, INTERESTS.INTEREST
- FROM DETAILS JOIN  INTERESTS ON DETAILS.NAME = INTERESTS.NAME
- Please generate a python code to execute this query. My mysql password is my-pwd. Output of the query must be written to a file name query_output.csv
'''

class DB_representation:
    def __init__(self):
        self.name = None
        self.type = None
        self.columns = list()
        self.column_datatype = list()
        self.special_case = None
        
    def sql_obj(self):
        self.name = "DETAILS"
        self.type = "mysql"
        self.columns = ["col1", "col1"]
        self.column_datatype = ["int", "str"]
        self.special_case = None
        self.admin_detail = ["MYSQL_DB" , "MYSQL_PWD", "MYSQL_USER", "MYSQL_HOST"]
        
    def mongo_obj(self):
        self.name = "INTERESTS"
        self.type = "mongodb"
        self.columns = ["col3", "col4"]
        self.column_datatype = ["float", "object"]
        self.special_case = None
        self.admin_detail = ["MONGO_HOST", "MONGO_DB"]


class Prompt:
    def __init__(self, sql_query):
        self.data_setup_spec = None
        self.schema_spec = None
        self.story_setup = "But the user of my data thinks all data is stored in mysql."
        self.sql_spec = None # TODO
        self.sql_query = sql_query
        self.query_spec = "With that assumption, they wrote the following query: " + self.sql_query
        self.output_spec = "Generate a python code to execute this query on my original data. Query output should be written to the file <insert>.csv"
        self.admin_spec = None

    def to_dict(self):
        member_variables = {attr: getattr(self, attr) for attr in dir(self) if not callable(getattr(self, attr)) and not attr.startswith("__")}
        for k, v in member_variables.items():
            print(f"* {k}: {v}")
        return member_variables

    def prompting(self, db_objs):
        temp = "I have a "
        for i in range(len(db_objs)):
            temp += f"{db_objs[i].type} table named {db_objs[i].name}"
            if i < len(db_objs)-1:
                temp += " and "
        print(temp)
        self.data_setup_spec = temp
        
        temp = ""
        for i in range(len(db_objs)):
            temp += f"{db_objs[i].name} has the columns "
            for j in range(len(db_objs[i].columns)):
                temp += f"{db_objs[i].columns[j]} of type {db_objs[i].column_datatype[j]}"
        self.schema_spec = temp
        
        temp = ""
        for i in range(len(db_objs)):
            temp += f"Details of my databases '{db_objs[i].name}' are as follows "
            for admin_d in db_objs[i].admin_detail:
                temp += admin_d + ", "
            if i < len(db_objs)-1:
                temp += " and "
        self.admin_spec = temp
        
class Multi_Message_ChatGPTQuery:
    def __init__(self):
        self.messages = list()
        self.input_message_len = list()
        self.data = ""
        self.runtime = -1
        self.output_text = ""
        self.gpt_model = "gpt-4" # "gpt-3.5-turbo"
        self.finished_reason = ""
        self.response = ""
        self.created_time = -1
        self.uid = ""
        self.completion_tokens = -1
        self.prompt_tokens = -1
        self.total_tokens = -1
        
    def set_input_message_len(self):
        assert len(self.input_message_len) == 0
        for msg in self.messages:
            self.input_message_len.append(len(msg))
    
    def add_context(self, new_msg, role="user"):
        formatted_msg ={"role": role, "content": new_msg}
        self.messages.append(formatted_msg)

    def chat_with_gpt(self):
        ###################################################
        gpt_response = openai.ChatCompletion.create(
            model=self.gpt_model,
            messages=self.messages,
            temperature=1,
            max_tokens=MAX_TOKEN,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        ###################################################
        # TODO: Which one is correct?
        # self.response = gpt_response.choices[0].text.strip()
        reason = gpt_response['choices'][0]['finish_reason']
        if reason != "stop":
            print("ERROR: GPT failed, finished_reason: {}")
            print("return None...")
            return None
        self.finished_reason = reason
        self.response = gpt_response['choices'][0]['message']['content']
        self.created_time = gpt_response["created"]
        self.uid = gpt_response["id"]
        self.completion_tokens = gpt_response["usage"]["completion_tokens"]
        self.prompt_tokens = gpt_response["usage"]["prompt_tokens"]
        self.total_tokens = gpt_response["usage"]["total_tokens"]
        print(f"gpt response: {gpt_response}")
        print(f"extracted response: {self.response}")
        return self.response
        
    def write_result(self):
        temp = list()
        assert len(self.messages) == len(self.input_message_len)
        temp.append(f"uid, {self.uid}")
        for i in range(len(self.messages)):
            temp.append(f"message_{i},{self.messages[i]},{self.input_message_len[i]}")
        temp.append(f"input_message_len,{self.input_message_len}")
        temp.append(f"MAX_TOKEN, {MAX_TOKEN}")
        temp.append(f"data, {self.data}")
        temp.append(f"runtime, {self.runtime}")
        temp.append(f"output_text, {self.output_text}")
        temp.append(f"gpt_model, {self.gpt_model}")
        temp.append(f"finished_reason, {self.finished_reason}")
        temp.append(f"response, {self.response}")
        temp.append(f"created_time, {self.created_time}")
        temp.append(f"completion_tokens, {self.completion_tokens}")
        temp.append(f"prompt_tokens, {self.prompt_tokens}")
        temp.append(f"total_tokens, {self.total_tokens}")
        path_ = util.get_current_time() + "-gpt_output.txt"  # Replace with the path to your file
        with open(path_, "w") as file:
            for elem in temp:
                file.write(elem + "\n")
    
    
class GPT:
    def __init__(self):
        self.num_query = 0
        # self.api_endpoint = 'https://api.openai.com/v1/engines/davinci-codex/completions'
    
    def send_request(self, cq):
        '''
        reference: https://platform.openai.com/docs/guides/gpt/chat-completions-api
        The system message helps set the behavior of the assistant. For example, you can modify the personality of the assistant or provide specific instructions about how it should behave throughout the conversation. However note that the system message is optional and the model’s behavior without a system message is likely to be similar to using a generic message such as "You are a helpful assistant."
        '''
        cq.set_input_message_len()
        result = cq.chat_with_gpt()
        print(result)
        ts = time.time()
        # response = requests.post(self.api_endpoint, json=cq.params, headers=cq.headers)
        # cq.data = response.json() # data is python dictionary. resopnse is json.
        assert cq.runtime == -1
        cq.runtime = (time.time() - ts)
        self.num_query += 1
        cq.write_result()
        return cq.response
    
        
    def call_chatgpt_api(self, query, schema):
        # q1 = f"Translate the following SQL query to MongoDB in python api: '{query}'"
        # q2 = f"This is schema of SQL table, '{schema}'"
        # q3 = f"Give me just one python code"

        cq = Multi_Message_ChatGPTQuery()
        cq.add_context(q1)
        cq.add_context(q2)
        cq.add_context(q3)
        gpt_output = self.send_request(cq)
        # mongodb_code = gpt_output['choices'][0]['text']
        print("********************")
        print("** chatgpt output **")
        print("********************")
        print(gpt_output)
    
        
def argparse_add_argument(parser):
    # parser.add_argument("--query", type=str, default=None, help="sql query that will be used for chatgpt", required=True)
    return 

if __name__ == "__main__":
    query = "SELECT CustomerName, City FROM Customers;"
    schema = "Customer SQL database has columns of CustomerName, City, and Address"
    gpt = GPT()
    gpt.call_chatgpt_api(query, schema)