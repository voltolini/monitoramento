#!/usr/bin/env python3
"""
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

# Desabilita avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================
# CONFIGURAÇÕES DO ZABBIX
# ============================================
ZBX_URL: str = "https://noc.ftd.com.br/zabbix/api_jsonrpc.php"
ZBX_TOKEN: str = "6feab728e78206b04dfbcfc593209a0d7d471f79c0cf2c3c429f100d9e051025"
VERIFY_SSL: bool = False

# CONFIGURAÇÕES DE EXPORTAÇÃO
EXPORT_EXCEL: bool = True   # Gerar Excel?
EXPORT_CSV: bool = True     # Gerar CSV?
# ============================================


class ZabbixHostExporter:
    """Classe para exportar hosts do Zabbix usando Token API"""
    
    def __init__(self, url: str, api_token: str, verify_ssl: bool = False):
        """Inicializa o exportador com Token API"""
        if 'api_jsonrpc.php' in url:
            self.url = url
        else:
            self.url = url.rstrip('/') + '/api_jsonrpc.php'
        
        self.api_token = api_token
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        
    def _make_request(self, method: str, params: Dict) -> Dict:
        """Faz requisição à API do Zabbix com tratamento de erros"""
        headers = {
            'Content-Type': 'application/json-rpc',
            'Authorization': f'Bearer {self.api_token}'
        }
        
        payload = {
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
            'id': 1
        }
            
        try:
            response = self.session.post(
                self.url,
                json=payload,
                headers=headers,
                verify=self.verify_ssl,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            
            if 'error' in result:
                error_msg = result['error'].get('data', result['error'].get('message', 'Erro desconhecido'))
                raise Exception(f"Erro da API Zabbix: {error_msg}")
                
            return result.get('result')
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Erro de conexão com Zabbix: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Erro ao decodificar resposta JSON: {str(e)}")
    
    def test_connection(self) -> bool:
        """Testa a conexão e valida o token"""
        print("🔐 Validando token de API...")
        
        try:
            # Primeiro testa a versão da API (sem autenticação)
            headers = {'Content-Type': 'application/json-rpc'}
            payload = {
                'jsonrpc': '2.0',
                'method': 'apiinfo.version',
                'params': {},
                'id': 1
            }
            response = self.session.post(self.url, json=payload, headers=headers, verify=self.verify_ssl, timeout=10)
            version_result = response.json()
            
            if 'result' in version_result:
                print(f"✅ API acessível! Versão do Zabbix: {version_result['result']}")
            
            # Agora testa o token tentando buscar informações do usuário
            user_result = self._make_request('user.get', {'output': ['userid', 'username']})
            
            if user_result:
                print(f"✅ Token válido! Usuário autenticado.")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ Erro na validação do token: {str(e)}")
            return False
    
    def get_all_hosts(self) -> List[Dict]:
        """Busca todos os hosts do Zabbix"""
        print("📡 Buscando hosts do Zabbix...")
        
        try:
            params = {
                'output': [
                    'hostid',
                    'host',
                    'name',
                    'status',
                    'available',
                    'error',
                    'maintenance_status',
                    'ipmi_available',
                    'snmp_available',
                    'description'
                ],
                'selectGroups': ['groupid', 'name'],
                'selectInterfaces': [
                    'interfaceid',
                    'ip',
                    'dns',
                    'port',
                    'type',
                    'main'
                ],
                'selectTags': ['tag', 'value'],
                'selectInventory': 'extend'
            }
            
            hosts = self._make_request('host.get', params)
            print(f"✅ {len(hosts)} hosts encontrados!")
            return hosts
            
        except Exception as e:
            print(f"❌ Erro ao buscar hosts: {str(e)}")
            return []
    
    def process_hosts_data(self, hosts: List[Dict]) -> pd.DataFrame:
        """Processa dados dos hosts e converte para DataFrame"""
        print("🔄 Processando dados dos hosts...")
        
        processed_data = []
        
        for host in hosts:
            # Informações básicas
            host_data = {
                'Host ID': host.get('hostid', ''),
                'Host Name': host.get('host', ''),
                'Visible Name': host.get('name', ''),
                'Status': 'Enabled' if host.get('status') == '0' else 'Disabled',
                'Availability': self._get_availability_status(host.get('available', '0')),
                'Maintenance': 'Yes' if host.get('maintenance_status') == '1' else 'No',
                'Description': host.get('description', ''),
                'Error Message': host.get('error', '')
            }
            
            # Grupos
            groups = host.get('groups', [])
            host_data['Groups'] = ', '.join([g.get('name', '') for g in groups])
            
            # Interface principal
            interfaces = host.get('interfaces', [])
            main_interface = next((i for i in interfaces if i.get('main') == '1'), None)
            
            if main_interface:
                host_data['IP Address'] = main_interface.get('ip', '')
                host_data['DNS Name'] = main_interface.get('dns', '')
                host_data['Port'] = main_interface.get('port', '')
                host_data['Interface Type'] = self._get_interface_type(main_interface.get('type', '1'))
            else:
                host_data['IP Address'] = ''
                host_data['DNS Name'] = ''
                host_data['Port'] = ''
                host_data['Interface Type'] = ''
            
            # Tags
            tags = host.get('tags', [])
            host_data['Tags'] = ', '.join([f"{t.get('tag', '')}:{t.get('value', '')}" for t in tags])
            
            # Inventário
            inventory = host.get('inventory', {})
            if inventory:
                host_data['OS'] = inventory.get('os', '')
                host_data['Location'] = inventory.get('location', '')
                host_data['Contact'] = inventory.get('contact', '')
                host_data['Hardware'] = inventory.get('hardware', '')
                host_data['Serial Number'] = inventory.get('serialno_a', '')
            
            processed_data.append(host_data)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ Dados processados: {len(df)} hosts")
        return df
    
    @staticmethod
    def _get_availability_status(status: str) -> str:
        """Converte código de disponibilidade para texto"""
        statuses = {
            '0': 'Unknown',
            '1': 'Available',
            '2': 'Unavailable'
        }
        return statuses.get(status, 'Unknown')
    
    @staticmethod
    def _get_interface_type(interface_type: str) -> str:
        """Converte código de tipo de interface para texto"""
        types = {
            '1': 'Agent',
            '2': 'SNMP',
            '3': 'IPMI',
            '4': 'JMX'
        }
        return types.get(interface_type, 'Unknown')
    
    def export_to_excel(self, df: pd.DataFrame, filename: str = None) -> str:
        """Exporta DataFrame para Excel"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'zabbix_hosts_{timestamp}.xlsx'
        
        print(f"💾 Exportando para Excel: {filename}")
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Hosts', index=False)
                
                # Ajusta largura das colunas
                worksheet = writer.sheets['Hosts']
                for idx, col in enumerate(df.columns):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(col)
                    ) + 2
                    worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
            
            print(f"✅ Arquivo Excel criado com sucesso!")
            return filename
            
        except Exception as e:
            print(f"❌ Erro ao criar Excel: {str(e)}")
            raise
    
    def export_to_csv(self, df: pd.DataFrame, filename: str = None) -> str:
        """Exporta DataFrame para CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'zabbix_hosts_{timestamp}.csv'
        
        print(f"💾 Exportando para CSV: {filename}")
        
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✅ Arquivo CSV criado com sucesso!")
            return filename
            
        except Exception as e:
            print(f"❌ Erro ao criar CSV: {str(e)}")
            raise


def main():
    """Função principal"""
    print("=" * 70)
    print("🚀 EXPORTADOR DE HOSTS DO ZABBIX 7 - GRUPO MARISTA")
    print("=" * 70)
    print()
    print(f"📍 Servidor: {ZBX_URL}")
    print(f"🔑 Token: {ZBX_TOKEN[:20]}...{ZBX_TOKEN[-10:]}")
    print()
    
    try:
        # Cria exportador
        exporter = ZabbixHostExporter(
            url=ZBX_URL,
            api_token=ZBX_TOKEN,
            verify_ssl=VERIFY_SSL
        )
        
        # Valida token
        if not exporter.test_connection():
            print("\n❌ Token inválido. Verifique as configurações.")
            sys.exit(1)
        
        print()
        
        # Busca hosts
        hosts = exporter.get_all_hosts()
        
        if not hosts:
            print("\n⚠️  Nenhum host encontrado ou erro na busca.")
            sys.exit(1)
        
        # Processa dados
        df = exporter.process_hosts_data(hosts)
        
        # Estatísticas
        print()
        print("=" * 70)
        print("📊 ESTATÍSTICAS DOS HOSTS")
        print("=" * 70)
        print(f"   📦 Total de hosts: {len(df)}")
        print(f"   ✅ Hosts habilitados: {len(df[df['Status'] == 'Enabled'])}")
        print(f"   ❌ Hosts desabilitados: {len(df[df['Status'] == 'Disabled'])}")
        print(f"   🟢 Hosts disponíveis: {len(df[df['Availability'] == 'Available'])}")
        print(f"   🔴 Hosts indisponíveis: {len(df[df['Availability'] == 'Unavailable'])}")
        print(f"   🔧 Hosts em manutenção: {len(df[df['Maintenance'] == 'Yes'])}")
        print("=" * 70)
        print()
        
        # Exporta arquivos
        arquivos_gerados = []
        
        if EXPORT_EXCEL:
            excel_file = exporter.export_to_excel(df)
            arquivos_gerados.append(excel_file)
        
        if EXPORT_CSV:
            csv_file = exporter.export_to_csv(df)
            arquivos_gerados.append(csv_file)
        
        # Resumo final
        print()
        print("=" * 70)
        print("✅ EXPORTAÇÃO CONCLUÍDA COM SUCESSO!")
        print("=" * 70)
        print()
        print("📁 Arquivos gerados:")
        for arquivo in arquivos_gerados:
            print(f"   • {arquivo}")
        print()
        print("💡 Os arquivos estão na mesma pasta deste script.")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Operação cancelada pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()