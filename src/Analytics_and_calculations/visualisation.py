import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from loguru import logger

from src.utils.time_script import time_stamp


# Функция для отображения графика на основе переданных данных.
#
# Параметры:
# - df (DataFrame): Данные для графика.
# - x (str): Название столбца данных для оси X.
# - y (str): Название столбца данных для оси Y.
# - title (str): Заголовок графика.
# - chart_type (str, optional): Тип графика ("line" или "bar"). По умолчанию - "line".
#
# График может быть двух типов - линейный (line) или столбчатый (bar).
# В зависимости от выбранного типа графика функция применяет разные параметры для отображения.


@time_stamp
def vis_to_queries(df, x, y, title, chart_type="line"):
    logger.info("Cоздание графика началось...")
    try:
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
            ax.set_xticks(df[x].unique())  # Установка отметок на оси x на основе часов

        plt.tight_layout()
        plt.show()

    except Exception as e:
        logger.error(f"Ошибка при создании графика: {e}")
