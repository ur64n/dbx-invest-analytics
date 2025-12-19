import logging

def get_logger(name: str = __name__) -> logging.Logger:

    logger = logging.getLogger(name)

    # zapobiega dodaniu wielu handlerów przy kolejnym imporcie
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # handler wypisujący na konsolę
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # opcjonalnie wyciszamy nadmiarowe logi bibliotek (np. Spark)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    return logger