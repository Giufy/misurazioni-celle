import matplotlib.pyplot as plt

from Database import Database
from ReaderFactory import ReaderFactory

df = ReaderFactory.get_dataframe_from_database(Database.get_default(), schema='raw', table='misurazioni_celle_v')
df.drop(columns=['ora',
                 'note',
                 'tipologia'], inplace=True)


def plt_moving_average(days: int = 7, cella_name: str = None, **kwargs):
    df.reset_index(inplace=True)
    df.set_index(keys=['data'], inplace=True)
    for cella in df['id'].drop_duplicates():
        if cella == cella_name or cella_name is None:
            tmp_df = df[df['id'] == cella]
            plt.plot(tmp_df['mV'].rolling(days).mean(), label=f'{cella} moving average {days}', **kwargs)
    plt.legend()
    # plt.figure()


def plt_average(days: int = 7, cella_name: str = None):
    df2 = df.groupby('id').apply(lambda x: x.resample(f'{days}D', on='data').mean())
    df2.reset_index(inplace=True)

    for cella in df2['id'].drop_duplicates():
        if cella == cella_name or cella_name is None:
            tmp_df = df2[df2['id'] == cella]
            plt.plot(tmp_df['data'], tmp_df['mV'], label=f'{cella} average')
    plt.legend()
    # plt.figure()


def plot_all(days: int = 7, cella_name: str = None):
    plt_average(days=days, cella_name=cella_name)
    plt_moving_average(days=days, cella_name=cella_name)


# plot_all(cella_name='OV2', days=2)


plt_moving_average(days=7, cella_name='OV2')
plt_moving_average(days=1, cella_name='OV2',marker = 'o',linestyle="None")
plt.xticks(rotation=45)
plt.show()
