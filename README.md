# Movies-ETL

## Purpose
Amazing Prime Video wants to develop an algorithm to determine which low budget movies being released will become popular so they can buy the streaming rights at a bargain.  They have decided to host a hackathon, providing a clean set of movie data and asking participants to predict the popular movies.  The purpose of this project is to create the dataset for the hackathon and to wirte the code in a way so that the data can updated easily on a daily basis.  The steps involved in creating the dataset are as follows:
1. Extract the data from the following data sources:
	1. Wikipedia data for all movies released since 1990
	2. Kaggle metadata
	3. MovieLens ratings data
2. Transform it into one clean dataset.
3. Load it into a PostreSQL database. 

## Resources
 - Data: movies_metadata.csv, ratings.csv, wikipedia-movies.json
 - Software: Python 3.8, Jupyter Notebook (anaconda3), PostgreSQL 12.4, pgAdmin 4

## Results
The analysis to extract, transform and load the dataset was completed in four main steps as follows.

1. The first step of the analysis was to write an ETL function that would read the three data files.  This was done using the json and pandas dependencies.  After the dependecies were imported, the function `extract_transform_load` was defined to take in three arguments, use the json method to load the json file, use the pandas method to read the csv's and convert the json data to a pandas DataFrameand then return the three dataframes as `wiki_movies_df`, `kaggle_metadata` and 'ratings`.  Rather than writing out the path to each file in the function, the path was defined as `file_dir` and referenced in each file path.  This will help in the future so that if the file path is changed it will only need to be updated in one location rather than multiple times.  See the script below. 
	```py
	# 1. Create a function that takes in three arguments;
	# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)

	def extract_transform_load(data1, data2, data3):
    		# 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    		kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)
    		ratings = pd.read_csv(f'{file_dir}/ratings.csv')
    
    		# 3. Open and read the Wikipedia data JSON file.
    		with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file: wiki_movies_raw = json.load(file)
    
    		# 4. Read in the raw wiki movie data as a Pandas DataFrame.
    		wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    		# 5. Return the three DataFrames
    		return wiki_movies_df, kaggle_metadata, ratings

	# 6 Create the path to your file directory and variables for the three files. 
	file_dir = 'C:/Users/whitn/Desktop/Data Analysis Bootcamp/Module 8/Movies-ETL/Resources'
	# Wikipedia data
	wiki_file = f'{file_dir}/wikipedia-movies.json'
	# Kaggle metadata
	kaggle_file = f'{file_dir}/movies_metadata.csv'
	# MovieLens rating data.
	ratings_file = f'{file_dir}/ratings.csv'

	# 7. Set the three variables in Step 6 equal to the function created in Step 1.
	wiki_file, kaggle_file, ratings_file = extract_transform_load(wiki_file, kaggle_file, ratings_file)
	```
2. The second step of the analysis was to Extract and Transform the Wikipedia Data.  To do this, a function was defined to take in the argument, `movie`, create a non-destructive copy of the argument, Combine all alternate titles of the movie into one column and merge columns of similar information such as `Directed by` and `Director`.  This provided a cleaned up database of the movie data.  See the script below.
	```py
	# 1. Add the clean movie function that takes in the argument, "movie".
	def clean_movie(movie):
    	    movie = dict(movie) # Create a non-destructive copy
   
    	    # Move all alternate titles into one column.
    	    alt_titles = {}
    	    for key in ['Also known as','Arabic','Cantonese','Chinese','French','Hangul','Hebrew','Hepburn','Japanese','Literally',
                	'Mandarin','McCune–Reischauer','Original title','Polish','Revised Romanization','Romanized', 'Russian',
                	'Simplified','Traditional','Yiddish']:
            	if key in movie:
            	    alt_titles[key] = movie[key]
            	    movie.pop(key)
    	    if len(alt_titles) > 0:
       		movie['alt_titles'] = alt_titles
    
    	# Merge columns of similar information
    	def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
            
    	change_column_name('Adaptation by', 'Written by')
    	change_column_name('Country of origin', 'Country')
    	change_column_name('Directed by', 'Director')
    	change_column_name('Distributed by', 'Distributor')
    	change_column_name('Edited by', 'Editor(s)')
    	change_column_name('Length', 'Running time')
    	change_column_name('Original release', 'Release date')
    	change_column_name('Music by', 'Composer(s)')
    	change_column_name('Produced by', 'Producer(s)')
    	change_column_name('Producer', 'Producer(s)')
    	change_column_name('Productioncompanies ', 'Production company(s)')
    	change_column_name('Productioncompany ', 'Production company(s)')
    	change_column_name('Released', 'Release date')
   	change_column_name('Screen story by', 'Writer(s)')
    	change_column_name('Screenplay by', 'Writer(s)')
    	change_column_name('Story by', 'Writer(s)')
    	change_column_name('Theme music composer', 'Composer(s)')
    	change_column_name('Written by', 'Writer(s)')
    
    	return movie
	```

   From there, the `extract_transform_load` function was run to load in the movie data.  To clean the loaded data, additional code was written within the function to extract the IMDb ID and drop any rows of duplicate ID's.  Next, the database was filtered to only keep columns which had less than 90% null values.  See the code below.
	```py
	try:
            wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
            wiki_movies_df.drop_duplicates(subset = 'imdb_id', inplace=True)
    	except Exception as e:
            print(f'imdb_id not found. Exception: {e}')

    	#  7. Write a list comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
    	wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    	wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
	```

   Next the "Box office" column data was cleaned.  The data was filtered to only include the non-null values from the column and the data was converted to a string data type using a lamda function.  The strings were then filtered using regular expression to match different elements of the box office data.  A function was then written to parse the matched data and convert it to a common format that can be used for analysis.  The old "Box office" column was dropped and the new data was renamed as "box_office".  See the code below.
	```py
	# 8. Create a variable that will hold the non-null values from the “Box office” column.
    	box_office = wiki_movies_df['Box office'].dropna()
    
    	# 9. Convert the box office data created in Step 8 to string values using the lambda and join functions.
    	box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    	# 10. Write a regular expression to match the six elements of "form_one" of the box office data.
    	form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'

    	# 11. Write a regular expression to match the three elements of "form_two" of the box office data.
    	form_two = r'\$\s*\d{1,3}(?:[,\.]\d{1,3})+(?!\s[mb]illion)'

    	# 12. Add the parse_dollars function.
    	def parse_dollars(s):
            # if s is not a string, return NaN
            if type(s) != str:
            	return np.nan
    
            # if input is of the form $###.# million
            if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            	# remove dollar sign and " million"
            	s = re.sub('\$|\s|[a-zA-Z]','', s)
            	# convert to float and multiply by a million
            	value = float(s) * 10**6
            	# return value
            	return value
    
            # if input is of the form $###.# billion
            elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            	# remove dollar sign and " billion"
            	s = re.sub('\$|\s|[a-zA-Z]','', s)
            	# convert to float and multiply by a billion
            	value = float(s) * 10**9
            	# return value
            	return value
    
            # if input is of the form $###,###,###
            elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            	# remove dollar sign and commas
            	s = re.sub('\$|,','', s)
            	# convert to float
            	value = float(s)
            	# return value
            	return value
    
            # otherwise, return NaN
            else:
            	return np.nan
        
    	# 13. Clean the box office column in the wiki_movies_df DataFrame.
    	box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
   	wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    	wiki_movies_df.drop('Box office', axis=1, inplace=True)
	```
   The same method was then repeated to clean the budget, release date and running time columns.  Each column was filtered to keep only the non-null values, the values were converted strings, the strings were filtered using regular expressions and then parsed and converted into a common format that could be used for data analysis.  The original columns were deleted and the new values were given new column names.  See the script below.
	```py
	# 14. Clean the budget column in the wiki_movies_df DataFrame.
    	budget = wiki_movies_df['Budget'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    	budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    	budget = budget.str.replace(r'\[\d+\]\s*', '')
    	wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    	# 15. Clean the release date column in the wiki_movies_df DataFrame.
    	release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    	date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    	date_form_two = r'\d{4}.[01]\d.[123]\d'
    	date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    	date_form_four = r'\d{4}'
    	wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    	# 16. Clean the running time column in the wiki_movies_df DataFrame.
    	running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
   	running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    	running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    	wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    	wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    	# Return three variables. The first is the wiki_movies_df DataFrame
    	return wiki_movies_df, kaggle_metadata, ratings
	```

3. The third step in the analysis was to extract and transform the Kaggle data.  To do this, the code was added to the extract_transform_load function to make it simlple to update the dataset in the future.  The first step of cleaning the Kaggle data was to drop any adult movies and update the data types of columns that weren't already in the correct data type.  Next the Kaggle data was merged with the Wiki data and reviewed using scatter plots to determine which data should be kept and which data could be eliminated.  After the analysis, it was determined that 4 columns could be dropped.  A function was then defined to fill in any missing Kaggle data using the Wikipedia data and then drop the wikipedia columns.  Missing data was filled in for the `runtime`, `budget_kaggle` and `revenue` columns.  The dataset was then filtered for only the useful columns, and the columns were renamed to make the table more clear.  See the script below.
	```py
	# 2. Clean the Kaggle metadata.
    	kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    	kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    	kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    	kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    	kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    	kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    	# 3. Merged the two DataFrames into the movies DataFrame.
    	movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    	# 4. Drop unnecessary columns from the merged DataFrame.
    	movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    	# 5. Add in the function to fill in the missing Kaggle data.
    	def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
            df.drop(columns=wiki_column, inplace=True)

    	# 6. Call the function in Step 5 with the DataFrame and columns as the arguments.
    	fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    	fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    	fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    	# 7. Filter the movies DataFrame for specific columns.
    	movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    	# 8. Rename the columns in the movies DataFrame.
    	movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
	```

4. The fourth and final step of the analysis was to clean the ratings data and create the movie database.  To do this, the `timestamp` data was converted to the `datetime` data type using the pandas method.  The ratings were then grouped by MovieID and rating, the count() method was used to count the number of times the movie was given each rating, the "userID" column was renamed as "count", and the dataframe was pivoted to make "movieID" the index.  The ratings dataframe was then merged with the Wikipedia and Kaggle data and all null values were filled with zero's.  See the script below.
	```py
	# 9. Transform and merge the ratings DataFrame.
    	ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    	# Group by movieID and rating, count the number of each rating, rename the 'userID' column as 'count' 
    	# and pivot the data to make movieId the index.
    	rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    	# Rename the columns
    	rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    	# Merge the ratings and movies dataframes.
    	movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    	# Fill missing data with zeros.
    	movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
	```
   To create the movie database, the python data was connected to the PostgreSQL database, the sqlalchemy method was used to create the database engine and the dataframe was saved to a SQL Table.  The data was then imported and the three variables were returned.  See the script below.
	```py
    	db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    
    	# Create the database engine.
    	engine = create_engine(db_string)
    
    	# Save the daataframe to a SQL table
    	movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    
    	# Import the data
    	rows_imported = 0
    	# get the start_time from time.time()
    	start_time = time.time()
    	for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='append')
            rows_imported += len(data)

            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
        
    	# Return three variables. The first is the wiki_movies_df DataFrame
    	return wiki_movies_df, kaggle_metadata, ratings 
	```

   Finally, when the function is called using the thee arguments, wiki_movies_df, kaggle_metadata and ratings, the database is created.

## Summary
The process of extracting and cleaning the data was messy and required a lot of behind the scenes work before refactoring the code to run in one function which can be used to update the dataset on a daily basis.  The thought process that went into filtering the data, determining which data to keep and which data to remove can be found in the Movies-ETL Lesson.ipynb file in the repository.  This file shows lists of column names, scatter plot comparisons of data, strings that weren't able to be parsed using the regular expressions, etc.  There are definitely things that could be done to clean the data more throroughly but the decision was made that the limited added value was not worth the time required.  The dataset created is sufficient for the needs of the Amazing Prime hackathon.