from sqlalchemy import create_engine

def executeQuery(user_name, password, host_name, db_name, query_str):
    print(query_str)
    db = getDbEngine(user_name, password, host_name, db_name)
    result_set = db.execute(query_str)  
    # for r in result_set:  
    #     print(r)
    return result_set

def getDbEngine(user_name, password, host_name, db_name):
    db_string = 'postgres://{}:{}@{}:5432/{}'.format(user_name, password, host_name, db_name)
    db = create_engine(db_string)
    return db
