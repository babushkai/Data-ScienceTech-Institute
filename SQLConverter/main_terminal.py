import subprocess
import sys
import argparse
import warnings
warnings.filterwarnings("ignore")
from database import SQLDatabase

# Install dependent packages(explicit)
try:
    import pandas as pd
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'pandas'])
finally:
    import pandas as pd

def printSplashScreen():
    print("\n\n")
    print("*************************************************************************************************")
    print("\t THIS SCRIPT ALLOWS TO EXTRACT SURVEY DATA FROM THE SAMPLE SEEN IN SQL CLASS")
    print("\t IT REPLICATES THE BEHAVIOUR OF A STORED PROCEDURE & TRIGGER IN A PROGRAMMATIC WAY")
    print("\t COMMAND LINE OPTIONS ARE:")
    print("\t\t -h or --help: print the help content on the console")
    print("*************************************************************************************************\n\n")

def main():
    print("\n--Input Values--")
    print(f"Driver name: [{args.driver_name}]")
    print(f"Surver name: [{args.server_name}]")
    print(f"Database name: [{args.database_name}]")
    print(f"View name: [{args.view_name}]")
    print(f"File name: [{args.file_name}]")

    # Create an instance of the SQLDatabase class
    print("\n--SQL DATABASE--")
    database = SQLDatabase(args.view_name, args.file_name, args.driver_name, args.server_name, args.database_name)

    # Get the list of IDs from the Survey table  
    survey_ids = database.get_survey_ids()
    # Get the list of IDs from the Question table
    question_ids = database.get_question_ids()
    
    InSurveyList = database.CheckQuestionInSurvey(survey_ids)

    # From the query of the function above, execute the query in SQL server to create InSurvey data frame to check late whether if QuestionId is InSurvey
    query_questionInSurvey = pd.read_sql_query(InSurveyList, database.sql_conn)
    df_questionInSurvey = pd.DataFrame(query_questionInSurvey)
    
    # From above SurveyId, QuestionId, InSurvey df, take only InSurvey == 1, 
    # Convert Pandas DataFrame into a list of SurveyId and QuestionId for those with InSurvey == 1
    df_InSurvey = df_questionInSurvey.loc[df_questionInSurvey['InSurvey']==1]
    SurveyQuestion_InSurvey_List = df_InSurvey.values[:,[0,1]].tolist()

    print('Survey ID and Question ID In and NOT In Survey Structure table:')
    print(df_questionInSurvey.values)
    print('Survey ID and Question ID that are in Survey Structure: \n' + str(SurveyQuestion_InSurvey_List))

    # Construct the SQL query
    survey_queries_list = []
    for s in survey_ids:
        question_list = []
        for q in question_ids:
            question_sql = database.strQueryTemplateForAnswerColumn(s, q, SurveyQuestion_InSurvey_List)
            question_list.append(question_sql)
        # Concatenate all questions
        dynamic_query = ' , '.join(question_list)
        # Create the outer part of the survey query using the dynamic query
        result = database.strQueryTemplateOuterUnionQuery(s, dynamic_query)
        # Append to the list of queries
        survey_queries_list.append(result)
    # Join all the survey queries with UNION, strFinalQuery is the final Query to get the result All Survey Data table
    strFinalQuery = " UNION ".join(survey_queries_list)
    
    #Execute the strFinalQuery in SQL to get the result All Survey Data
    query_strFinalQuery = pd.read_sql_query(strFinalQuery, database.sql_conn)
    df_strFinalQuery = pd.DataFrame(query_strFinalQuery)

    # Get the previous Survey Structure
    df_savedSurveyStructure = database.get_survey_structure()

    # Modify the saved Survey Structure (delete last row)
    df_savedSurveyStructure2 = df_savedSurveyStructure.drop(df_savedSurveyStructure.index[3])

    # Get New Survey Structure
    df_newSurveyStructure = database.get_survey_structure()

    # Compare Survey Structure. If different, activate the trigger to create view and CSV file with final All Survey data
    # else do nothing
    if database.compareStructure(df_savedSurveyStructure2, df_newSurveyStructure) == True:
        print('New SurveyStructure is different from the saved one, need to trigger view')
        database.createViewSQL(args.view_name, strFinalQuery, database.sql_conn)
        database.createCSV(args.file_name, df_strFinalQuery)
    else:
        print('New SurveyStructure is same as saved one, do nothing')

if __name__ == '__main__':
    try:
         # Set argument parsers
        parser = argparse.ArgumentParser(description="SQL Database connection")
        parser.add_argument('-v', '--view_name', type=str, help='Input view')
        parser.add_argument('-f', '--file_name', type=str, help='Output csv file')
        parser.add_argument('-d', '--driver_name', type=str, help='Driver to connect')
        parser.add_argument('-s', '--server_name', type=str, help='Server to connect')
        parser.add_argument('-db', '--database_name', type=str, help='Database to connect')
        args = parser.parse_args()
    except Exception as e:
        print("Command Line arguments processing error: " + str(e))
    printSplashScreen()
    main()