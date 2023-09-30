import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from src.utils.time_script import time_stamp

@time_stamp
def vis_to_queries(df, x, y, title, chart_type="line"):
    fig, ax = plt.subplots(figsize=(12, 6))
    if chart_type == "line":
        ax = df.plot(x=x, y=y, kind=chart_type, ax=ax, legend=False, colormap='viridis', marker='o')
    else:
        ax = df.plot(x=x, y=y, kind=chart_type, ax=ax, legend=False, colormap='viridis')
    ax.set_title(title)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    ax.set_xlabel(x)
    ax.set_ylabel(y)

    if chart_type == "bar":
        # Чтобы разделить цифры слева на первом графике
        ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

        # Перемещение цифр снизу, чтобы избежать наложения
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

        # Перемещение цифр рядом со столбцами за границы сетки
        rects = ax.patches
        for rect, label in zip(rects, df[y]):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height + 5, label,
                    ha='center', va='bottom')
    else:
        ax.get_yaxis().set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
        ax.set_xticks(df[x].unique())  # Set x-ticks based on unique hours

    plt.tight_layout()
    plt.show()
