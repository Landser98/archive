# src/utils/warnings_setup.py
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="src.utils.income_calc")
