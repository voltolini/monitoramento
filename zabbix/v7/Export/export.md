Script para exportar todos os hosts do Zabbix 7
Versão com autenticação por TOKEN API - Pronto para uso
Autor: Script personalizado
Data: 2024
"""

import requests
import pandas as pd
import json
import sys
from datetime import datetime
from typing import Dict, List
import urllib3
