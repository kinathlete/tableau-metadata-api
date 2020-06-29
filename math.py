import simplemath as sm
import pandas as pd

def do_math(df):
	
	x = df.iloc[0]["x"]
	y = df.iloc[0]["y"]

	data = {'result' : sm.multiply(x, y) }

	return pd.DataFrame(data, index=[0])
	
def get_output_schema():
	return pd.DataFrame({
		'result':prep_int()
	})