import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import pandas as pd

from Database import Database
from ReaderFactory import ReaderFactory

df = ReaderFactory.get_dataframe_from_database(Database.get_default(), schema='raw', table='misurazioni_celle_v')
df.drop(columns=['ora',
                 'note',
                 'tipologia',
                 'giorni_attivita'], inplace=True)

df = df[(df['tipo'] == 'M') | (df['tipo'] == '1:4M') | (df['tipo'] == '2:3M')]


def custom_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(['tipo', 'data']).agg(['mean', 'std'])


class ColorFactory:

    def __init__(self):
        self.index = 0
        print(dict(mcolors.TABLEAU_COLORS))
        self.colors = [v for k, v in mcolors.TABLEAU_COLORS.items()]

    def color(self):
        color = self.colors[self.index]
        self.index += 1
        return color


df = custom_aggregate(df)


def plt_moving_average(df: pd.DataFrame, parameter: str, days: int = 7, ax=None, **kwargs):
    if ax is None:
        fig, ax = plt.subplots(1, 1)
    colorFactory = ColorFactory()

    df = df.reset_index(inplace=False)
    for cella in df['tipo'].drop_duplicates():
        color = colorFactory.color()

        moving_avg = get_moving_average(df, days, parameter, cella)

        ax.fill_between(moving_avg['data'], moving_avg['upper'], moving_avg['lower'], color=color, alpha=.1,)

        ax.plot(moving_avg['data'], moving_avg['mean'], label=f'{cella} moving average {days}', color=color,
                **kwargs)
    ax.legend()
    ax.set_title(parameter)

    for tick in ax.get_xticklabels():
        tick.set_rotation(45)


def get_moving_average(df, days, parameter, cella):
    tmp_df = df[df['tipo'] == cella]
    moving_avg = pd.DataFrame()
    moving_avg['data'] = tmp_df['data']
    moving_avg['mean'] = tmp_df[parameter]['mean'].rolling(days).mean()
    moving_avg['std'] = tmp_df[parameter]['std'].rolling(days).mean()
    moving_avg['upper'] = moving_avg['mean'] + moving_avg['std']
    moving_avg['lower'] = moving_avg['mean'] - moving_avg['std']
    return moving_avg


fig, axs = plt.subplots(2, 2)
parameters = ['mV', 'mA_picco', 'mA_5s', 'microW']

for i in range(len(parameters)):
    index = i + 1
    x = index % 2
    y = 1 if index > 2 else 0

    plt_moving_average(df=df, parameter=parameters[i], days=1, ax=axs[x, y])

    plt.plot()
plt.show()
