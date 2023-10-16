import requests
import time
import datetime
import openai
import os

os.environ['OPENAI_API_KEY'] = "YOUR API TOKEN"
openai.api_key = os.getenv("OPENAI_API_KEY")

class Utility:
    def __init__(self):
        pass
    
    def get_current_time(self):
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d-%H-%M-%S")
        return formatted_datetime
util = Utility()


# '''
# data format:
# {
#     "choices": [
#         {
#             "text": "db.users.find({ age: { $gt: 30 } });"
#         }
#     ]
# }
# '''
# class Single_Message_ChatGPTQuery:
#     def __init__(self, input_params, headers):
#         self.input_params = input_params
#         self.input_prompt_len = len(self.input_params["prompt"])
#         self.max_output_token_len = len(self.input_params["max_tokens"])
        
        
#         self.headers = headers
#         self.response = ""
#         self.runtime = -1
#         self.output_text = ""
#         self.output_token_len = -1
#         self.context = ""
#         # self.tqs = -1
        
#     def set_output_text(self):
#         assert self.output_text == ""
#         assert self.output_token_len == -1
#         self.output_text = self.response['choices'][0]['text']
#         self.output_token_len = len(self.output_text)
#         return self.output_text
    
#     def write_result(self):
#         temp = list()
#         temp.append("input_params,"+self.input_params)
#         temp.append("input_prompt_len,"+self.input_prompt_len)
#         temp.append("max_output_tokens,"+self.max_output_tokens)
#         temp.append("headers,"+self.headers)
#         temp.append("response,"+self.response)
#         temp.append("runtime,"+self.runtime)
#         temp.append("output_text,"+self.output_text)
#         temp.append("output_token_len,"+self.output_token_len)
#         temp.append("context,"+self.context)
#         path_ = util.get_current_time() + "-" + self.input_params["prompt"][:8] + ".txt"  # Replace with the path to your file
#         with open(path_, "w") as file:
#             for elem in temp:
#                 file.write(elem + "\n")


'''
<api>
response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Who won the world series in 2020?"},
        {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
        {"role": "user", "content": "Where was it played?"}
    ]
)
<response>
{
    "choices": [
        {
        "finish_reason": "stop",
        "index": 0,
        "message": {
            "content": "The 2020 World Series was played in Texas at Globe Life Field in Arlington.",
            "role": "assistant"
        }
        }
    ],
    "created": 1677664795,
    "id": "chatcmpl-7QyqpwdfhqwajicIEznoc6Q47XAyW",
    "model": "gpt-3.5-turbo-0613",
    "object": "chat.completion",
    "usage": {
        "completion_tokens": 17,
        "prompt_tokens": 57,
        "total_tokens": 74
    }
}

response['choices'][0]['message']['content']
'''
        
class Multi_Message_ChatGPTQuery:
    def __init__(self, mt):
        self.messages = list()
        self.input_message_len = list()
        self.max_output_token_len = mt
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
    
    def add_context(self, role, new_msg):
        def create_open_ai_msg(r, ms):
            return {"role": r, "content": ms}
        open_ai_msg = create_open_ai_msg(role, new_msg)
        self.messages.append(open_ai_msg)

    def chat_with_gpt(self):
        ###################################################
        gpt_response = openai.ChatCompletion.create(
            model=self.gpt_model,
            messages=self.messages,
            temperature=1,
            max_tokens=self.max_output_token_len,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        ###################################################
        # TODO: Which one is correct?
        # self.response = gpt_response.choices[0].text.strip()
        reason = gpt_response['choices'][0]['finish_reason']
        if reason != "stop":
            print("GPT failed, finished_reason: {}")
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
        temp.append(f"max_output_token_len, {self.max_output_token_len}")
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
        path_ = util.get_current_time() + "-" + self.input_params["prompt"][:8] + ".txt"  # Replace with the path to your file
        with open(path_, "w") as file:
            for elem in temp:
                file.write(elem + "\n")
    
    
class GPT:
    def __init__(self, api_k, max_t):
        self.num_query = 0
        self.api_key = api_k # ChatGPT api key
        self.max_token = max_t     
        # Define the API endpoint
        self.api_endpoint = 'https://api.openai.com/v1/engines/davinci-codex/completions'
    
    def send_request(self, cq, list_of_msg):
        '''
        reference: https://platform.openai.com/docs/guides/gpt/chat-completions-api
        The system message helps set the behavior of the assistant. For example, you can modify the personality of the assistant or provide specific instructions about how it should behave throughout the conversation. However note that the system message is optional and the model’s behavior without a system message is likely to be similar to using a generic message such as "You are a helpful assistant."
        '''
        for msg in list_of_msg:
            cq.add_context(msg)
        cq.set_input_message_len()
        result = cq.chat_with_gpt()
        print(result)
        ts = time.time()
        response = requests.post(self.api_endpoint, json=cq.params, headers=cq.headers)
        assert cq.runtime == -1
        cq.runtime = (time.time() - ts)
        self.num_query += 1
        cq.data = response.json() # data is python dictionary. resopnse is json.
        cq.write_result()
        return cq.response
    
        
    def call_chatgpt_api(self, sql_query):
        params = {
            'prompt': f"Translate the following SQL query to MongoDB: '{sql_query}'",
            'max_tokens': self.max_token  # The max length of the generated text
        }
        # Set up headers with your API key
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        # cq = Single_Message_ChatGPTQuery(params, headers)
        cq = Multi_Message_ChatGPTQuery(params, headers)
        gpt_output = self.send_request(cq)
        mongodb_code = gpt_output['choices'][0]['text']
        print(mongodb_code)