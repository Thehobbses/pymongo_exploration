import PullFromMongoDB
import pandas as pd
import matplotlib.pyplot as plt


def main():
    dataframes = PullFromMongoDB.main(PullFromMongoDB.extract_database_list)

    # grab index dictionary and interpret as df
    index_dicts = dataframes['newspaper_index']
    index_df = pd.DataFrame.from_dict(index_dicts['newspaper_index-0'])

    # grab details dictionaries and interpret as df
    details_dicts = dataframes['newspaper_details']
    details_df = pd.DataFrame.from_dict(details_dicts['newspaper_details-0'])
    daily_df = pd.DataFrame.from_dict(details_dicts['newspaper_details-1'])

    # change some column formats and extract months and years
    daily_df['date_issued'] = pd.to_datetime(daily_df['date_issued'])
    daily_df['year'] = pd.DatetimeIndex(daily_df['date_issued']).year
    daily_df['month'] = pd.DatetimeIndex(daily_df['date_issued']).month
    daily_df['state'] = daily_df['state'].astype(object)

    grouped_df = daily_df.groupby(['year', 'state']).size().unstack(fill_value=0)

    def pandas_plotter(df: pd.DataFrame):
        """
        handles the plotting of a pandas dataframe object
        :param df: pandas dataframe
        :return: plotted points
        """
        fig, axes = plt.subplots(nrows=2, ncols=2, subplot_kw={'ylim': (0, 3000)})

        df1 = df.iloc[:, [i for i in range(0, 13)]]
        df2 = df.iloc[:, [i for i in range(14, 27)]]
        df3 = df.iloc[:, [i for i in range(28, 41)]]
        df4 = df.iloc[:, [i for i in range(42, 53)]]

        df1_plot = df1.plot.line(xlabel='Date', ylabel='Count', figsize=(25, 18), ax=axes[0, 0])
        df2_plot = df2.plot.line(xlabel='Date', ylabel='Count', figsize=(25, 18), ax=axes[0, 1])
        df3_plot = df3.plot.line(xlabel='Date', ylabel='Count', figsize=(25, 18), ax=axes[1, 0])
        df4_plot = df4.plot.line(xlabel='Date', ylabel='Count', figsize=(25, 18), ax=axes[1, 1])

        df1_plot.legend(loc='upper left')
        df2_plot.legend(loc='upper left')
        df3_plot.legend(loc='upper left')
        df4_plot.legend(loc='upper left')

        plt.suptitle(t='Daily Newspapers Published by State')
        return plt

    plot_obj = pandas_plotter(grouped_df)
    plot_obj.savefig('newspaper_freq.png')


if __name__ == "__main__":
    main()
