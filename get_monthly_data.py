import pandas as pd
import os

def load_data() -> pd.DataFrame:
    data = pd.read_csv(r'data\NTNU Sbygg Eksport-20211022T000125-4.csv', sep=';')
    return data

def calculate_specific_deltas(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['Timestamp', 'Summarized Yearly yield']].copy()
    
    if len(df) > 0:
        first_timestamp = df['Timestamp'].iloc[0]
        if first_timestamp.day == 1 and first_timestamp.month == 1:
            df['Summarized Yearly yield'] = df['Summarized Yearly yield'].diff()
            df = df.iloc[1:].reset_index(drop=True)
            df.iloc[0, 1] = 0
        else:
            df['Summarized Yearly yield'] = df['Summarized Yearly yield'].diff()
            df.iloc[0, 1] = 0
    else:
        print("DataFrame is empty after diff operation.")
        return pd.DataFrame()
    
    return df



def reformat_data(data: pd.DataFrame) -> pd.DataFrame:
    if len(data) < 4:
        return pd.DataFrame()  # Return empty DataFrame

    try:
        columns = data.iloc[1][1:-1].values.tolist()
    except IndexError:
        return pd.DataFrame()

    columns.insert(0, "Timestamp")
    reformatted_data = pd.DataFrame(columns=columns, data=data.iloc[3:, :-1].values)
    reformatted_data['Timestamp'] = pd.to_datetime(reformatted_data['Timestamp'], errors='coerce')

    for column in reformatted_data.columns[1:]:
        reformatted_data[column] = reformatted_data[column].str.replace('.', '').str.replace(',', '.').astype(float)

    return reformatted_data

def get_daily_production(data: pd.DataFrame) -> pd.DataFrame:
    data['Date'] = data['Timestamp'].dt.date
    numeric_columns = data.select_dtypes(include='number').columns
    daily_data = data.groupby('Date')[numeric_columns].sum().reset_index()
    return daily_data

def get_daily_data() -> pd.DataFrame:
    daily_data = pd.DataFrame()
    for file in os.listdir('data'):
        if file.endswith('4.csv'):
            try:
                data = pd.read_csv(f'data/{file}', sep=';')
                reformatted_data = reformat_data(data)
                if reformatted_data.empty:
                    print(f"Data is empty for file: {file}")
                    continue
                df_with_deltas = calculate_specific_deltas(reformatted_data)
                if df_with_deltas.empty:
                    print(f"Delta calculation resulted in empty DataFrame for file: {file}")
                    continue
                data = get_daily_production(df_with_deltas)
                daily_data = pd.concat([daily_data, data])
            except Exception as e:
                print(f"Error in file: {file}, {e}")
    return daily_data

def get_monthly_production() -> pd.DataFrame:
    daily_data = get_daily_data()
    daily_data['Month'] = pd.to_datetime(daily_data['Date']).dt.to_period('M')
    numeric_columns = daily_data.select_dtypes(include='number').columns.difference(['Date'])
    monthly_averages = daily_data.groupby('Month')[numeric_columns].sum().reset_index()
    return monthly_averages


def main():
    monthly_averages = get_monthly_production()
    print(monthly_averages)


if __name__ == '__main__':
    main()



"""
NOTES:
sammenligne sum med siste verdi for året

"""