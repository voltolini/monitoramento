#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================
# CONFIGURAÇÕES (EDITE AQUI)
# ==========================
ZBX_URL: str = "https://magna.grupomarista.org.br/zabbix/api_jsonrpc.php"
ZBX_TOKEN: str = "138586b974f9e955e4562a5c3bb591394415d336c9f4a209fdd409bb88230b12"  # Users → API tokens

# Caminho do Excel (.xlsx) com a aba "hosts"
INPUT_PATH = r"C:\Users\andre.baptista.ext\Desktop\zbx_hosts_template.xlsx"

# Verificação TLS: True (verifica), False (ignora), ou caminho para .pem
VERIFY_TLS: Union[bool, str] = False

# Timeout para requisições (segundos)
TIMEOUT: int = 30

# Atualizar hosts existentes? True = atualiza, False = pula
UPDATE_EXISTING: bool = False

# Configurações de retry
REQUESTS_MAX_RETRIES: int = 5
REQUESTS_BACKOFF_FACTOR: float = 0.6

# ID do grupo padrão caso não especificado (geralmente "2" = "Linux servers")
DEFAULT_GROUP_ID: str = "2"

# Modo verboso (mais logs)
VERBOSE: bool = True

# Diretório para logs e relatórios
OUTPUT_DIR: Optional[str] = None  # None = mesmo diretório do Excel

# Colunas esperadas no Excel
COLUMNS = [
    "host", "name", "ip", "groups", "templates", "proxy_group",
    "interface_type", "port", "use_ip", "dns", "tags", "macros", "description"
]

# ==========================
# CONFIGURAÇÃO DE LOGGING
# ==========================
def setup_logging(output_dir: str, verbose: bool = False) -> logging.Logger:
    """Configura logging para arquivo e console"""
    logger = logging.getLogger("ZabbixImport")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    
    # Handler para arquivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_dir, f"zabbix_import_{timestamp}.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(fh)
    
    # Handler para console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(ch)
    
    logger.info(f"Log iniciado: {log_file}")
    return logger

# ==========================
# EXCEÇÕES CUSTOMIZADAS
# ==========================
class ZabbixAPIError(RuntimeError):
    """Erro retornado pela API do Zabbix"""
    def __init__(self, method: str, code: int, message: str, data: Optional[str] = None):
        self.method = method
        self.code = code
        self.message = message
        self.data = data
        msg = f"Zabbix API error {code} on {method}: {message}"
        if data:
            msg += f" | data: {data}"
        super().__init__(msg)

class ValidationError(ValueError):
    """Erro de validação de dados"""
    pass

# ==========================
# FUNÇÕES AUXILIARES
# ==========================
def normalize_api_url(url: str) -> str:
    """Normaliza URL da API do Zabbix"""
    u = url.strip()
    return u if u.endswith("/api_jsonrpc.php") else u.rstrip("/") + "/api_jsonrpc.php"

def safe_str(value, default: str = "") -> str:
    """Converte valor para string de forma segura"""
    if value is None or pd.isna(value):
        return default
    return str(value).strip()

def _boolish(v) -> bool:
    """Converte valores diversos para boolean"""
    if isinstance(v, bool):
        return v
    if pd.isna(v):
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "sim", "s", "on")

def _map_interface_type(val) -> int:
    """Mapeia tipo de interface para código numérico"""
    s = safe_str(val, "1").lower()
    
    if s.isdigit():
        t = int(s)
        return t if t in (1, 2, 3, 4) else 1
    
    # Mapeamento de strings
    mapping = {
        "agent": 1, "zabbix": 1, "zabbix agent": 1, "agentd": 1,
        "snmp": 2, "snmpv2": 2, "snmpv3": 2,
        "ipmi": 3,
        "jmx": 4
    }
    return mapping.get(s, 1)

def validate_host_name(host: str) -> str:
    """Valida e sanitiza nome do host"""
    if not host or not host.strip():
        raise ValidationError("Nome do host é obrigatório")
    
    host = host.strip()
    
    # Caracteres não permitidos no Zabbix
    invalid_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', ' ']
    for char in invalid_chars:
        if char in host:
            raise ValidationError(f"Nome do host contém caractere inválido: '{char}'")
    
    if len(host) > 128:
        raise ValidationError(f"Nome do host muito longo (max 128 caracteres): {len(host)}")
    
    return host

# ==========================
# CLASSE API ZABBIX
# ==========================
class ZabbixAPI:
    """Cliente para API do Zabbix 7.x com suporte a proxy_group"""
    
    def __init__(self, url: str, token: str, timeout: int = 30, 
                 verify_tls: Union[bool, str] = True,
                 max_retries: int = 3, backoff_factor: float = 0.3,
                 logger: Optional[logging.Logger] = None):
        self.url = normalize_api_url(url)
        self.token = token
        self.timeout = timeout
        self.verify_tls = verify_tls
        self._rid = 0
        self.logger = logger or logging.getLogger("ZabbixAPI")
        
        # Configurar sessão com retry
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json-rpc"})
        
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        
        if self.verify_tls is False:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    def _request(self, method: str, params: dict) -> dict:
        """Faz requisição à API do Zabbix"""
        self._rid += 1
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "auth": self.token,
            "id": self._rid
        }
        
        self.logger.debug(f"API Request [{self._rid}]: {method}")
        self.logger.debug(f"Params: {json.dumps(params, indent=2)}")
        
        try:
            r = self._session.post(
                self.url, 
                data=json.dumps(payload), 
                timeout=self.timeout, 
                verify=self.verify_tls
            )
            r.raise_for_status()
        except requests.exceptions.Timeout:
            raise RuntimeError(f"Timeout ao chamar {method} (>{self.timeout}s)")
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(f"Erro de conexão ao chamar {method}: {e}")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Erro HTTP ao chamar {method}: {e}")
        
        try:
            data = r.json()
        except ValueError:
            raise RuntimeError(f"Resposta inválida (não é JSON) de {method}: {r.text[:300]}")
        
        if "error" in data:
            err = data["error"]
            raise ZabbixAPIError(
                method, 
                err.get("code", -1), 
                err.get("message", "Unknown"), 
                err.get("data")
            )
        
        result = data.get("result")
        self.logger.debug(f"API Response [{self._rid}]: {type(result)}")
        return result
    
    def api_version(self) -> str:
        """Retorna versão da API"""
        r = self._session.post(
            self.url,
            data=json.dumps({
                "jsonrpc": "2.0",
                "method": "apiinfo.version",
                "params": {},
                "id": 1
            }),
            timeout=self.timeout,
            verify=self.verify_tls
        )
        r.raise_for_status()
        j = r.json()
        if "error" in j:
            raise RuntimeError(f"Erro ao obter versão: {j['error']}")
        return j["result"]
    
    def ping_auth(self) -> dict:
        """Testa autenticação"""
        return self._request("user.get", {"output": ["userid", "username"], "limit": 1})
    
    # ---- LOOKUPS ----
    
    def get_host_by_host(self, host: str) -> Optional[dict]:
        """Busca host por nome (technical name)"""
        try:
            res = self._request("host.get", {
                "filter": {"host": [host]},
                "selectGroups": ["groupid", "name"],
                "selectInterfaces": "extend",
                "selectParentTemplates": ["templateid", "name"],
                "output": "extend",
                "limit": 1
            })
            return res[0] if res else None
        except ZabbixAPIError as e:
            self.logger.error(f"Erro ao buscar host '{host}': {e}")
            return None
    
    def ensure_groups(self, groups: List[str]) -> List[Dict[str, str]]:
        """Garante que grupos existam, criando se necessário"""
        groups = [g.strip() for g in groups if g and str(g).strip()]
        if not groups:
            return []
        
        self.logger.debug(f"Verificando grupos: {groups}")
        
        # Buscar grupos existentes
        existing = self._request("hostgroup.get", {
            "output": ["groupid", "name"],
            "filter": {"name": groups}
        })
        by_name = {g["name"]: g["groupid"] for g in existing}
        
        # Criar grupos faltantes
        missing = [g for g in groups if g not in by_name]
        if missing:
            self.logger.info(f"Criando grupos: {missing}")
            self._request("hostgroup.create", [{"name": g} for g in missing])
        
        # Buscar novamente para pegar IDs
        final = self._request("hostgroup.get", {
            "output": ["groupid", "name"],
            "filter": {"name": groups}
        })
        
        return [{"groupid": g["groupid"]} for g in final]
    
    def resolve_templates(self, template_names: List[str]) -> List[Dict[str, str]]:
        """Resolve nomes de templates para IDs"""
        template_names = [t.strip() for t in template_names if t and str(t).strip()]
        if not template_names:
            return []
        
        self.logger.debug(f"Resolvendo templates: {template_names}")
        
        res = self._request("template.get", {
            "output": ["templateid", "name"],
            "filter": {"name": template_names}
        })
        
        found = {t["name"] for t in res}
        missing = [t for t in template_names if t not in found]
        
        if missing:
            raise ValidationError(f"Templates não encontrados: {', '.join(missing)}")
        
        name_to_id = {t["name"]: t["templateid"] for t in res}
        return [{"templateid": name_to_id[name]} for name in template_names]
    
    def resolve_proxy_group(self, proxy_group_name: Optional[str]) -> Optional[str]:
        """
        Resolve nome do proxy group para ID (Zabbix 7.x)
        No Zabbix 7.x, existe proxy_group separado de proxy
        """
        if not proxy_group_name or not str(proxy_group_name).strip():
            return None
        
        pgn = str(proxy_group_name).strip()
        
        # Ignorar se parece URL
        if pgn.lower().startswith(("http://", "https://")) or "/" in pgn:
            self.logger.warning(f"Valor '{pgn}' parece URL, ignorando proxy_group")
            return None
        
        self.logger.debug(f"Resolvendo proxy group: {pgn}")
        
        try:
            # Zabbix 7.x usa proxygroup.get
            proxy_groups = self._request("proxygroup.get", {
                "output": ["proxy_groupid", "name"]
            })
            
            if not proxy_groups:
                self.logger.warning("Nenhum proxy group retornado pela API")
                return None
            
            # Match exato
            for pg in proxy_groups:
                if pg.get("name") == pgn:
                    self.logger.info(f"Proxy group encontrado: {pgn} (ID: {pg['proxy_groupid']})")
                    return pg["proxy_groupid"]
            
            # Match case-insensitive
            low = pgn.lower()
            for pg in proxy_groups:
                if pg.get("name", "").lower() == low:
                    self.logger.info(f"Proxy group encontrado (case-insensitive): {pgn} (ID: {pg['proxy_groupid']})")
                    return pg["proxy_groupid"]
            
            # Match parcial
            for pg in proxy_groups:
                if low in pg.get("name", "").lower():
                    self.logger.info(f"Proxy group encontrado (parcial): {pgn} → {pg['name']} (ID: {pg['proxy_groupid']})")
                    return pg["proxy_groupid"]
            
            # Listar disponíveis
            available = ", ".join(sorted([pg.get("name", "<sem nome>") for pg in proxy_groups]))
            raise ValidationError(f"Proxy group '{pgn}' não encontrado. Disponíveis: {available}")
            
        except ZabbixAPIError as e:
            # Se método não existe, pode ser versão incompatível
            if "method" in str(e).lower() or "not found" in str(e).lower():
                self.logger.error("Método proxygroup.get não disponível. Confirme que está usando Zabbix 7.x")
            raise
    
    # ---- HOST OPERATIONS ----
    
    def create_host(self, host: str, visible_name: Optional[str], groups: List[Dict[str, str]],
                    interfaces: List[dict], templates: Optional[List[Dict[str, str]]] = None,
                    proxy_groupid: Optional[str] = None, description: Optional[str] = None,
                    tags: Optional[List[dict]] = None, macros: Optional[List[dict]] = None) -> str:
        """Cria novo host"""
        params = {
            "host": host,
            "groups": groups,
            "interfaces": interfaces
        }
        
        if visible_name:
            params["name"] = visible_name
        if templates:
            params["templates"] = templates
        if proxy_groupid:
            params["proxy_groupid"] = proxy_groupid  # Zabbix 7.x
        if description:
            params["description"] = description
        if tags:
            params["tags"] = tags
        if macros:
            params["macros"] = macros
        
        self.logger.debug(f"Criando host: {host}")
        res = self._request("host.create", params)
        return res["hostids"][0]
    
    def update_host(self, hostid: str, visible_name: Optional[str], groups: Optional[List[Dict[str, str]]],
                    interfaces: Optional[List[dict]], templates: Optional[List[Dict[str, str]]] = None,
                    proxy_groupid: Optional[str] = None, description: Optional[str] = None,
                    tags: Optional[List[dict]] = None, macros: Optional[List[dict]] = None) -> str:
        """Atualiza host existente"""
        params = {"hostid": hostid}
        
        if visible_name is not None:
            params["name"] = visible_name
        if groups is not None:
            params["groups"] = groups
        if interfaces is not None:
            params["interfaces"] = interfaces
        if templates is not None:
            params["templates"] = templates
        if proxy_groupid is not None:
            params["proxy_groupid"] = proxy_groupid  # Zabbix 7.x
        if description is not None:
            params["description"] = description
        if tags is not None:
            params["tags"] = tags
        if macros is not None:
            params["macros"] = macros
        
        self.logger.debug(f"Atualizando host ID: {hostid}")
        res = self._request("host.update", params)
        return res["hostids"][0]

# ==========================
# PARSERS EXCEL
# ==========================
def parse_semicolon_list(cell: Optional[str]) -> List[str]:
    """Parse lista separada por ponto-e-vírgula"""
    val = safe_str(cell)
    if not val:
        return []
    return [x.strip() for x in val.split(";") if x.strip()]

def parse_tags(cell: Optional[str]) -> List[dict]:
    """Parse tags no formato: tag1=valor1;tag2=valor2"""
    val = safe_str(cell)
    if not val:
        return []
    
    tags = []
    for pair in val.split(";"):
        pair = pair.strip()
        if not pair:
            continue
        
        if "=" in pair:
            k, v = pair.split("=", 1)
            tags.append({"tag": k.strip(), "value": v.strip()})
        else:
            tags.append({"tag": pair, "value": ""})
    
    return tags

def parse_macros(cell: Optional[str]) -> List[dict]:
    """Parse macros no formato: {$MACRO1}=valor1;{$MACRO2}=valor2"""
    val = safe_str(cell)
    if not val:
        return []
    
    macros = []
    for pair in val.split(";"):
        pair = pair.strip()
        if not pair:
            continue
        
        if "=" in pair:
            k, v = pair.split("=", 1)
            macro_name = k.strip()
            # Garantir que macro está no formato {$...}
            if not macro_name.startswith("{$"):
                macro_name = "{$" + macro_name.lstrip("{$")
            if not macro_name.endswith("}"):
                macro_name = macro_name.rstrip("}") + "}"
            
            macros.append({"macro": macro_name, "value": v.strip()})
    
    return macros

def build_interface(row: pd.Series, logger: logging.Logger) -> dict:
    """Constrói objeto de interface do host"""
    t = _map_interface_type(row.get("interface_type", "1"))
    use_ip = _boolish(row.get("use_ip", "yes"))
    
    # Portas padrão por tipo
    default_ports = {1: "10050", 2: "161", 3: "623", 4: "12345"}
    default_port = default_ports.get(t, "10050")
    
    port = safe_str(row.get("port", ""), default_port)
    if not port:
        port = default_port
    
    ip_val = safe_str(row.get("ip", ""))
    dns_val = safe_str(row.get("dns", ""))
    
    # Validações
    if use_ip and not ip_val:
        raise ValidationError("Campo 'ip' é obrigatório quando use_ip=yes")
    if not use_ip and not dns_val:
        raise ValidationError("Campo 'dns' é obrigatório quando use_ip=no")
    
    # Validar formato IP
    if use_ip and ip_val:
        parts = ip_val.split(".")
        if len(parts) != 4 or not all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
            logger.warning(f"IP '{ip_val}' pode estar em formato inválido")
    
    # Validar porta
    try:
        port_num = int(port)
        if port_num < 1 or port_num > 65535:
            raise ValidationError(f"Porta inválida: {port} (deve estar entre 1-65535)")
    except ValueError:
        raise ValidationError(f"Porta inválida: {port} (deve ser número)")
    
    interface = {
        "type": t,
        "main": 1,
        "useip": 1 if use_ip else 0,
        "ip": ip_val if use_ip else "",
        "dns": "" if use_ip else dns_val,
        "port": port
    }
    
    logger.debug(f"Interface construída: tipo={t}, ip={ip_val}, dns={dns_val}, porta={port}")
    return interface

def read_excel_hosts(path: str, logger: logging.Logger) -> pd.DataFrame:
    """Lê planilha Excel com dados dos hosts"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    
    logger.info(f"Lendo Excel: {path}")
    
    # Tentar ler aba "hosts"
    try:
        df = pd.read_excel(path, sheet_name="hosts", dtype=str)
        logger.info("Aba 'hosts' encontrada")
    except ValueError:
        # Se não existir, pegar primeira aba
        df = pd.read_excel(path, dtype=str)
        logger.warning("Aba 'hosts' não encontrada, usando primeira aba")
    
    # Preencher vazios e normalizar colunas
    df = df.fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    
    # Validar colunas obrigatórias
    required = {"host", "groups"}
    missing = required - set(df.columns)
    if missing:
        raise ValidationError(f"Colunas obrigatórias faltando no Excel: {', '.join(missing)}")
    
    # Adicionar colunas opcionais faltantes
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""
            logger.debug(f"Coluna '{col}' não encontrada, adicionada como vazia")
    
    # Remover linhas completamente vazias
    df = df[df["host"].str.strip() != ""]
    
    logger.info(f"Total de linhas válidas: {len(df)}")
    return df[COLUMNS]

# ==========================
# PROCESSADOR
# ==========================
def process_row(api: ZabbixAPI, row: pd.Series, update: bool, 
                default_group_id: str, logger: logging.Logger) -> Tuple[str, str, str]:
    """
    Processa uma linha do Excel
    Retorna: (host, status, detalhes)
    """
    try:
        # Validar e extrair nome do host
        host = validate_host_name(safe_str(row["host"]))
        
        # Extrair campos
        visible_name = safe_str(row.get("name", "")) or None
        description = safe_str(row.get("description", "")) or None
        
        groups_names = parse_semicolon_list(row.get("groups", ""))
        template_names = parse_semicolon_list(row.get("templates", ""))
        proxy_group_name = safe_str(row.get("proxy_group", "")) or None
        
        tags = parse_tags(row.get("tags", ""))
        macros = parse_macros(row.get("macros", ""))
        
        # Resolver referências
        groups = api.ensure_groups(groups_names) if groups_names else [{"groupid": default_group_id}]
        templates = api.resolve_templates(template_names) if template_names else []
        proxy_groupid = api.resolve_proxy_group(proxy_group_name) if proxy_group_name else None
        
        # Construir interface
        interfaces = [build_interface(row, logger)]
        
        # Verificar se host já existe
        existing = api.get_host_by_host(host)
        
        if existing is None:
            # Criar novo host
            hostid = api.create_host(
                host=host,
                visible_name=visible_name,
                groups=groups,
                interfaces=interfaces,
                templates=templates or None,
                proxy_groupid=proxy_groupid,
                description=description,
                tags=tags or None,
                macros=macros or None
            )
            return host, "CRIADO", f"Host criado com ID {hostid}"
        
        else:
            # Host já existe
            if update:
                hostid = existing["hostid"]
                api.update_host(
                    hostid=hostid,
                    visible_name=visible_name,
                    groups=groups,
                    interfaces=interfaces,
                    templates=templates if templates else None,
                    proxy_groupid=proxy_groupid,
                    description=description,
                    tags=tags if tags else None,
                    macros=macros if macros else None
                )
                return host, "ATUALIZADO", f"Host atualizado (ID {hostid})"
            else:
                hostid = existing["hostid"]
                return host, "EXISTENTE", f"Host já existe (ID {hostid}), pulado"
    
    except ValidationError as ve:
        host_name = safe_str(row.get("host", "<sem nome>"))
        return host_name, "ERRO_VALIDAÇÃO", str(ve)
    
    except ZabbixAPIError as ze:
        host_name = safe_str(row.get("host", "<sem nome>"))
        return host_name, f"ERRO_API_{ze.code}", f"{ze.message}"
    
    except Exception as e:
        host_name = safe_str(row.get("host", "<sem nome>"))
        logger.exception(f"Erro inesperado processando '{host_name}'")
        return host_name, "ERRO", str(e)

# ==========================
# RELATÓRIO
# ==========================
def generate_report(results: List[dict], output_dir: str, logger: logging.Logger) -> str:
    """Gera relatório CSV dos resultados"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(output_dir, f"zabbix_import_resultado_{timestamp}.csv")
    
    try:
        df = pd.DataFrame(results)
        df.to_csv(report_file, index=False, encoding="utf-8-sig")  # BOM para Excel
        logger.info(f"Relatório gerado: {report_file}")
        return report_file
    except Exception as e:
        logger.error(f"Erro ao gerar relatório: {e}")
        return ""

def print_summary(results: List[dict], logger: logging.Logger):
    """Imprime resumo da execução"""
    from collections import Counter
    
    status_count = Counter(r["status"] for r in results)
    
    logger.info("\n" + "="*60)
    logger.info("RESUMO DA IMPORTAÇÃO")
    logger.info("="*60)
    logger.info(f"Total de hosts processados: {len(results)}")
    logger.info("")
    
    for status, count in sorted(status_count.items()):
        logger.info(f"  {status}: {count}")
    
    logger.info("="*60)

# ==========================
# MAIN
# ==========================
def main():
    """Função principal"""
    
    # Validar configurações básicas
    if not ZBX_URL or ZBX_URL == "COLOQUE_SEU_TOKEN_AQUI":
        print("ERRO: Configure ZBX_URL nas configurações do script", file=sys.stderr)
        sys.exit(1)
    
    if not ZBX_TOKEN or ZBX_TOKEN == "COLOQUE_SEU_TOKEN_AQUI":
        print("ERRO: Configure ZBX_TOKEN nas configurações do script", file=sys.stderr)
        sys.exit(1)
    
    if not INPUT_PATH.lower().endswith(".xlsx"):
        print("ERRO: INPUT_PATH deve ser um arquivo .xlsx", file=sys.stderr)
        sys.exit(1)
    
    # Determinar diretório de saída
    output_dir = OUTPUT_DIR or os.path.dirname(os.path.abspath(INPUT_PATH))
    os.makedirs(output_dir, exist_ok=True)
    
    # Configurar logging
    logger = setup_logging(output_dir, VERBOSE)
    
    try:
        logger.info("="*60)
        logger.info("INICIANDO IMPORTAÇÃO DE HOSTS PARA ZABBIX 7.x")
        logger.info("="*60)
        logger.info(f"URL: {ZBX_URL}")
        logger.info(f"Excel: {INPUT_PATH}")
        logger.info(f"Atualizar existentes: {UPDATE_EXISTING}")
        logger.info("")
        
        # Conectar à API
        logger.info("Conectando à API do Zabbix...")
        api = ZabbixAPI(
            url=ZBX_URL,
            token=ZBX_TOKEN,
            timeout=TIMEOUT,
            verify_tls=VERIFY_TLS,
            max_retries=REQUESTS_MAX_RETRIES,
            backoff_factor=REQUESTS_BACKOFF_FACTOR,
            logger=logger
        )
        
        # Verificar versão e autenticação
        try:
            version = api.api_version()
            logger.info(f"Versão da API: {version}")
            
            if not version.startswith("7."):
                logger.warning(f"ATENÇÃO: Este script foi otimizado para Zabbix 7.x. Versão detectada: {version}")
            
            user_info = api.ping_auth()
            logger.info(f"Autenticação OK - Usuário conectado")
            logger.info("")
            
        except ZabbixAPIError as ze:
            logger.error(f"Erro de autenticação: {ze}")
            logger.error("Verifique se o token está correto e tem permissões adequadas")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Erro ao conectar: {e}")
            sys.exit(2)
        
        # Ler planilha
        logger.info("Lendo planilha Excel...")
        try:
            df = read_excel_hosts(INPUT_PATH, logger)
            logger.info(f"Total de hosts a processar: {len(df)}")
            logger.info("")
        except Exception as e:
            logger.error(f"Erro ao ler Excel: {e}")
            sys.exit(3)
        
        # Processar hosts
        logger.info("Processando hosts...")
        logger.info("-"*60)
        
        results = []
        success_count = 0
        error_count = 0
        
        for idx, row in df.iterrows():
            row_num = idx + 2  # +2 porque Excel começa em 1 e tem cabeçalho
            host_name = safe_str(row.get("host", f"linha_{row_num}"))
            
            try:
                host, status, details = process_row(
                    api=api,
                    row=row,
                    update=UPDATE_EXISTING,
                    default_group_id=DEFAULT_GROUP_ID,
                    logger=logger
                )
                
                results.append({
                    "linha_excel": row_num,
                    "host": host,
                    "status": status,
                    "detalhes": details
                })
                
                if status in ("CRIADO", "ATUALIZADO"):
                    logger.info(f"✓ [{row_num}] {host}: {status}")
                    success_count += 1
                elif status == "EXISTENTE":
                    logger.info(f"⊙ [{row_num}] {host}: {status}")
                    success_count += 1
                else:
                    logger.error(f"✗ [{row_num}] {host}: {status} - {details}")
                    error_count += 1
                
            except Exception as e:
                logger.exception(f"Erro inesperado na linha {row_num} ({host_name})")
                results.append({
                    "linha_excel": row_num,
                    "host": host_name,
                    "status": "ERRO_CRÍTICO",
                    "detalhes": str(e)
                })
                error_count += 1
        
        logger.info("-"*60)
        logger.info("")
        
        # Gerar relatório
        report_file = generate_report(results, output_dir, logger)
        
        # Mostrar resumo
        print_summary(results, logger)
        
        if report_file:
            logger.info(f"\nRelatório detalhado: {report_file}")
        
        # Código de saída
        if error_count > 0:
            logger.warning(f"\nImportação concluída com {error_count} erro(s)")
            sys.exit(1)
        else:
            logger.info("\nImportação concluída com sucesso!")
            sys.exit(0)
    
    except KeyboardInterrupt:
        logger.warning("\n\nImportação interrompida pelo usuário")
        sys.exit(130)
    
    except Exception as e:
        logger.exception("Erro fatal na execução")
        sys.exit(4)

if __name__ == "__main__":
    main()