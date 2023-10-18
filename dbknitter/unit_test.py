import gpt as gpt

def test_prompt_class():
    mysql = gpt.DB_representation()
    mysql.name = "DETAILS"
    mysql.type = "mysql"
    mysql.columns = ["AGE", "INTEREST"]
    mysql.column_datatype = ["int", "str"]
    mysql.special_case = None
    mysql.admin_detail = ["MYSQL_DB" , "MYSQL_PWD", "MYSQL_USER", "MYSQL_HOST"]
    
    mongodb = gpt.DB_representation()
    mongodb.name = "INTERESTS"
    mongodb.type = "mongodb"
    mongodb.columns = ["NAME", "INTEREST"]
    mongodb.column_datatype = ["str", "str"]
    mongodb.special_case = None
    mongodb.admin_detail = ["MONGO_HOST", "MONGO_DB"]
    
    sql_query = "SELECT DETAILS.AGE, INTERESTS.INTEREST FROM DETAILS JOIN  INTERESTS ON DETAILS.NAME = INTERESTS.NAME"
    pmt = gpt.Prompt(sql_query)
    pmt.prompting([mysql, mongodb])
    pmt.to_dict()
    
if __name__ == "__main__":
    test_prompt_class() # Pass