import pandas as pd

def load_data() -> pd.DataFrame:
    data = pd.read_csv(r'data\NTNU Sbygg Eksport-20211022T000125-4.csv', sep=';')
    return data

def reformat_data(data: pd.DataFrame) -> pd.DataFrame:
    columns = data.iloc[1][1:-1].values.tolist()
    columns.insert(0, "Timestamp")
    reformatted_data = pd.DataFrame(columns=columns, data=data.iloc[3:, :-1].values)
    return reformatted_data

def main():
    data = load_data()
    reformated_data = reformat_data(data)
    print(reformated_data)

if __name__ == '__main__':
    main()
