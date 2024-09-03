import pandas as pd
import os

def load_data() -> pd.DataFrame:
    data = pd.read_csv(r'data\NTNU Sbygg Eksport-20211022T000125-4.csv', sep=';')
    return data

def calculate_specific_deltas(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['Timestamp', 'Summarized Total yield', 'Summarized Yearly yield']].copy()
    
    df['Summarized Total yield'] = df['Summarized Total yield'].diff()
    df['Summarized Yearly yield'] = df['Summarized Yearly yield'].diff()
    
    df.iloc[0, 1] = 0
    df.iloc[0, 2] = 0
    
    return df

def reformat_data(data: pd.DataFrame) -> pd.DataFrame:
    columns = data.iloc[1][1:-1].values.tolist()
    columns.insert(0, "Timestamp")
    reformatted_data = pd.DataFrame(columns=columns, data=data.iloc[3:, :-1].values)
    
    reformatted_data['Timestamp'] = pd.to_datetime(reformatted_data['Timestamp'])
    
    for column in reformatted_data.columns[1:]:
        reformatted_data[column] = reformatted_data[column].str.replace('.', '').str.replace(',', '.').astype(float)
    
    return reformatted_data

def get_daily_average(data: pd.DataFrame) -> pd.DataFrame:
    data['Date'] = data['Timestamp'].dt.date
    numeric_columns = data.select_dtypes(include='number').columns
    daily_averages = data.groupby('Date')[numeric_columns].mean().reset_index()
    return daily_averages

def get_daily_averages() -> pd.DataFrame:
    daily_averages = pd.DataFrame()
    for file in os.listdir('data'):
        if file.endswith('4.csv'):
            try:
                data = pd.read_csv(f'data/{file}', sep=';')
                reformatted_data = reformat_data(data)
                df_with_deltas = calculate_specific_deltas(reformatted_data)
                daily_average = get_daily_average(df_with_deltas)
                daily_averages = pd.concat([daily_averages, daily_average])
            except Exception as e:
                print(f"Error in file: {file}")
    return daily_averages

def get_monthly_averages() -> pd.DataFrame:
    daily_averages = get_daily_averages()
    daily_averages['Month'] = pd.to_datetime(daily_averages['Date']).dt.to_period('M')
    numeric_columns = daily_averages.select_dtypes(include='number').columns.difference(['Date'])
    monthly_averages = daily_averages.groupby('Month')[numeric_columns].mean().reset_index()
    return monthly_averages


def main():
    monthly_averages = get_monthly_averages()
    print(monthly_averages)


if __name__ == '__main__':
    main()
