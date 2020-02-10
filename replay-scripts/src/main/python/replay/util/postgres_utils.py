from sqlalchemy import create_engine

def executeQuery(db_name, query_str):
    print(query_str)
    db = getDbEngine(db_name)
    result_set = db.execute(query_str)  
    return result_set

def getDbEngine(db_name):
    user_name = os.environ['POSTGRES_USERNAME']
    password = os.environ['POSTGRES_PASSWORD']
    host_name = os.environ['POSTGRES_HOSTNAME']
    db_string = 'postgres://{}:{}@{}:5432/{}'.format(user_name, password, host_name, db_name)
    db = create_engine(db_string)
    return db
