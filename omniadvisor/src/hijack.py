import sys
from omniadvisor.utils.logger import logger
from omniadvisor.interface.hijack_recommend import hijack_recommend


def main():
    if len(sys.argv) != 2:
        logger.error(f"Excepted 1 argument but receive {len(sys.argv) - 1} arguments")
        raise ValueError(f"Excepted 1 arguments but receive {len(sys.argv) - 1}")
    hijack_recommend(sys.argv[1])


if __name__ == '__main__':
    main()
