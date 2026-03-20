# -*- coding: utf-8 -*-
"""
Created on Tue Aug  5 13:53:36 2025

@author: pipel
"""
import requests
import sys
import pandas as pd

from datetime import date
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from dotenv import load_dotenv
import os
from zoneinfo import ZoneInfo
from pprint import pprint
import time
from pathlib import Path


###############################################################################
# Validar que la ejecución ocurra solo en días hábiles de Chile

hoy_chile = datetime.now(ZoneInfo("America/Santiago")).date()

if hoy_chile.weekday() >= 5:  # sábado=5, domingo=6
    print(f"""
Hoy en Chile es: {hoy_chile.strftime('%Y-%m-%d')}
No es día hábil, por lo tanto el script no se ejecutará.
""")
    sys.exit()

print(f"""
Hoy en Chile es: {hoy_chile.strftime('%Y-%m-%d')}
Es día hábil, el script continuará ejecutándose.
""")



###############################################################################
# La idea de este script va a ser decirle a la persona que esta enviando el
# efectivo cuanto efectivo recibio el dia de ayer, cuanto tiene que enviar
# y que confirme enviando el arqueo de caja respectivo. Luego tambien tiene que
# preparar el depostito ella misma.

# Vamos a seguir los siguientes pasos:
#    1) Vamos a saber que dia es hoy (que dia se ejecuta el script). Si hoy es
#       dia 0 (lunes) vamos a restarle dos y para ir a buscar el dia viernes.
#    2) Vamos a preparar la URL para ir a buscar todos los pagos recibidos ORCT
#       del dia anterior que se hicieron en efectivo. Buscamos: CardCode, LicTradNum
#       CardName, FolioPref, FolioNum, DocTotal, ORCT.CashSum, ORCT.NoDocSum (ley de redondeo),
#       U_SEI_VUEL, (ORCT.CashSum + U_SEI_VUEL), DocDate
#
#    3) Ya en el pago en efectivo buscamos "PaymentInvoices": [{"DocEntry": 19966}]
#       y guardamos el valor del DocEntry
#    4) con ese valor vamos a b1s/v1/Invoinces(19966) y desde ahi sacamos: CardCode
#       CardName, CreationDate, DocTime, DocTotal, U_SEI_RED, U_SEI_VUEL, U_SEI_Code
#       UserSignz
#    5) luego mandamos el mail: El efectivo EXACTO a depositar, el IdCaja,
#       que responda este mismo mail de con quien fue a depositar
###############################################################################





BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


###############################################################################
# Paso -1: Vamos a obtener el día de hoy y vamos a obtener el ultimo dia habil



# vamos a buscar 'dia_para_buscar_efectivo' que es el dia que vamos a buscar el efectivo
if date.today().weekday() == 0:  # los lunes son 0
    dia_para_buscar_efectivo = date.today() - timedelta(days=3)  # asi buscamos el viernes anterior
else:
    dia_para_buscar_efectivo = date.today() - timedelta(days=1)  # ayer


# buscamos un mes atras para filtrar la OBNK despues 
un_mes_atras = dia_para_buscar_efectivo - relativedelta(days=30)

# formateamos las fechas 
dia_para_buscar_efectivo = dia_para_buscar_efectivo.strftime('%Y-%m-%d')
un_mes_atras = un_mes_atras.strftime('%Y-%m-%d')

# aca dejamos la opcion para hardcodear la fecha si es necesario, pero lo dejamos comentado
# dia_para_buscar_efectivo = '2025-10-13'
# un_mes_atras = '2025-07-01'


print(f"""
Hoy es: {date.today().strftime('%Y-%m-%d')}
Vamos a buscar efectivo el: {dia_para_buscar_efectivo}
Hace 30 dias era: {un_mes_atras}
""")



###############################################################################
# Paso 0: hacemos login en SAP con usuario

url_login = f"{os.getenv('BASE_URL_SAP')}/Login" # Endpoint de login
credenciales = { # Le pasamos las credenciales para loguearnos con un usuario en SAP 
    "CompanyDB": os.getenv("CompanyDB"),  # Nos estamos conectando a la base prodctiva de LARA
    "Password": os.getenv("pw_admin03"),
    "UserName": os.getenv("user_admin03")
                }

response = requests.post(url_login, json=credenciales, verify=True) 

# si el login no es exitoso, entonces interrumpimos el codigo de inmediato. 
if response.status_code not in range(200, 299+1):
    print(f"Error en el response: {response.status_code}")
    print(f"El texto del error es: {response.text}")
    sys.exit()
else:
    print(f"""\nLogin usuario {credenciales.get('UserName')} a SAP exitoso.\n""")
    
session_token = response.cookies['B1SESSION'] # Buscamos el value segun la key B1SESSION para el id de la sesion


headers = {"Content-Type" : "application/json",
        "Cookie": f"B1SESSION={session_token}"}


###############################################################################
# Paso 1: vamos a configurar la URL del con el dia para buscar Pagos Recibidos en efectivo
# la ORCT DocDate = 'dia_para_buscar_efectivo' = '2025-08-01'
# Tambien vamos a manejar el caso que no hayan pagos en efectivo el dia 'dia_para_buscar_efectivo'



###############################################################################
# Caso no hay Pagos Recibidos en efectivo el 'dia_para_buscar_efectivo', consultamos
# la OBNK para saber cual fue el ultimo deposito y a qué caja corresponde. 




url_ORCT = f"{os.getenv('BASE_URL_SAP')}/IncomingPayments"

columnas_url_ORCT = {
    "$filter": f"CashSum ne 0 and DocDate eq '{dia_para_buscar_efectivo}'", # Buscamos pagos recibidos en efectivo para el dia 'dia_para_buscar_efectivo'
    "$select": "CardCode,CardName,DocDate,CashSum,U_SEI_Code,PaymentInvoices" # estas son las columnas que sacamos
}

# Vamos a buscar los Pagos Recibidos en efectivo del dia 'dia_para_buscar_efectivo'
response_ORCT = requests.get(url_ORCT, headers=headers, params=columnas_url_ORCT, verify=True)

# Si el json que retorna el response de la ORCT está vacio, no hay Pagos Recibidos en efectivo para el día 'dia_para_buscar_efectivo'
# asique vamos a pasar a buscar la fecha del ultimo deposito y a cual es la caja que justifica ese deposito 
if not response_ORCT.json()['value']: 
    print(f"\nNO EXISTEN Pagos Recibidos en efectivo - ORCT el día {datetime.strptime(dia_para_buscar_efectivo, '%Y-%m-%d').strftime('%d-%m-%Y')}\n")
    print(f"\nVamos a pasar a buscar el ultimo deposito en la cartola OBNK entre: {datetime.strptime(un_mes_atras, '%Y-%m-%d').strftime('%d-%m-%Y')} y {datetime.strptime(dia_para_buscar_efectivo, '%Y-%m-%d').strftime('%d-%m-%Y')}")
    url_OBNK = f"{os.getenv('BASE_URL_SAP')}/BankPages"

    columnas_url_OBNK = {
       "$filter": f"AccountCode eq '11020004' and DueDate ge '{un_mes_atras}' and DueDate le '{dia_para_buscar_efectivo}' and BankMatch gt 0", # Buscamos en la cuenta del Security, entre 'un_mes_atras' y 'dia_para_buscar_efectivo'
       "$select": "DueDate,BankMatch,Memo,DebitAmount,CreditAmount,Sequence,ExternalCode", # estas son las columnas que sacamos
       "$skip": 0
    }
    df_OBNK = pd.DataFrame() # creamos el df_OBNK vacio antes de comenzar el while

    while True:
        print(f"Recuperando datos... Offset actual: {columnas_url_OBNK['$skip']}")  # Nueva línea para mostrar el progreso

        response_OBNK = requests.get(url_OBNK, headers=headers, params=columnas_url_OBNK, verify=True)

        if response_OBNK.status_code not in range(200, 300): # en el range() entra cualquier numero entre 200 y 299 
           print(f"Error: status code: {response_OBNK.status_code}")
           sys.exit()

        data = response_OBNK.json()
        df_batch_OBNK = pd.DataFrame(data["value"], columns=["DueDate","BankMatch", "Memo","DebitAmount","CreditAmount","Sequence", "ExternalCode"])
        
        if df_batch_OBNK.empty:
            print("No hay más registros para recuperar. Finalizando bucle.")  # Nueva línea para indicar que terminó
            break  # No hay más registros, salir del bucle
        
        df_OBNK = pd.concat([df_OBNK, df_batch_OBNK], ignore_index=True)
        
        print(f"Registros obtenidos en esta iteración: {len(df_batch_OBNK)}")  # Nueva línea para ver cuántos registros se recuperaron
        
        columnas_url_OBNK["$skip"] += len(df_batch_OBNK)  # Preparar para saltar los registros ya recuperados

    
    # damos formato fecha a la columna "DueDate"
    df_OBNK['DueDate'] = pd.to_datetime(df_OBNK['DueDate']).dt.strftime('%Y-%m-%d')
    df_OBNK = df_OBNK.sort_values(by='DueDate',ascending=False)
    
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    
    print(f"""
    Los ultimos 30 días de depositos en 11020004 - Banco Security que ya estan reconciliados se ven asi
    {df_OBNK}
          
    """)
    
    # Transformarmos "DueDate" en key y "BankMatch" en value 
    dict_df_OBNK = df_OBNK.groupby('DueDate')['BankMatch'].apply(list).to_dict()
    
    print("""
    El diccionario con los numeros de reconciliacion de los ultimos depositos agrupados por dia se ven asi: 
    """)
    pprint(dict_df_OBNK)# ponemos el print pretty en otra linea





    # Transformamos las keys del dict_df_OBNK en datetime para poder quedarnos con la más reciente
    dict_df_OBNK_DueDate_datetime = {datetime.strptime(fecha, '%Y-%m-%d'): valores for fecha, valores in dict_df_OBNK.items()}
        
    # Paso 1: Obtener la key con fecha más reciente
    fecha_mas_reciente = max(dict_df_OBNK_DueDate_datetime.keys())
    
    # Paso 2: Obtener todos los BankMatch de esa fecha
    lista_bankmatch = dict_df_OBNK_DueDate_datetime[fecha_mas_reciente]
    
    # Inicializamos lista para guardar los idApertura
    lista_additional_reference_validos = []
    # Otro diccionario para llegar al FintocId de la cartola, desde el IdApertura. 
    lista_sub_diccionario_FintocId_OBNK_con_IdApertura = []
    
    
    # 🔹 Ordenar las fechas del diccionario de forma descendente (más reciente primero)
    fechas_ordenadas = sorted(dict_df_OBNK_DueDate_datetime.keys(), reverse=True)
    
    # 🔹 Inicializamos las listas finales (se mantendrán entre fechas)
    lista_additional_reference_validos = []
    lista_sub_diccionario_FintocId_OBNK_con_IdApertura = []
    
    # 🔁 Recorremos cada fecha hasta encontrar resultados válidos
    for fecha_actual in fechas_ordenadas:
        print(f"\n📅 Procesando fecha: {fecha_actual.date()}")
        lista_bankmatch = dict_df_OBNK_DueDate_datetime[fecha_actual]
    
        # Lista temporal por fecha (para saber si encontramos algo en esta fecha)
        id_validos_en_fecha = []
    
        # Paso 2.1 al 4 — Recorrer cada BankMatch
        for cada_bankmatch in lista_bankmatch:
            print(f"\n🔁 Procesando BankMatch: {cada_bankmatch}")
    
            # Paso 2.1 - Obtener TransactionNumber desde OMTH
            url_OMTH = f"{os.getenv('BASE_URL_SAP')}/ExternalReconciliationsService_GetReconciliation"
            payload_OMTH = {
                "ExternalReconciliationParams": {
                    "AccountCode": "11020004",
                    "ReconciliationNo": cada_bankmatch
                }
            }
            response_OMTH = requests.post(url_OMTH, json=payload_OMTH, headers=headers, verify=True)
    
            try:
                trans_id = response_OMTH.json()['ReconciliationJournalEntryLines'][0]['TransactionNumber']
                sub_diccionario_FintocId_OBNK_con_IdApertura = {
                    'idApertura' : response_OMTH.json()['ReconciliationJournalEntryLines'][0]['Ref3'],
                    'FintocId' : response_OMTH.json()['ReconciliationJournalEntryLines'][0]['Ref2']
                }
                print(f"✅ TransactionNumber encontrado: {trans_id}")
            except (KeyError, IndexError):
                print(f"❌ No se pudo obtener TransactionNumber para BankMatch {cada_bankmatch}")
                continue
    
            # Paso 3: Entrar al asiento contable (OJDT)
            url_OJDT = f"{os.getenv('BASE_URL_SAP')}/JournalEntries({trans_id})"
            params_OJDT = {"$select": "OriginalJournal,JournalEntryLines"}
            response_OJDT = requests.get(url_OJDT, headers=headers, params=params_OJDT, verify=True)
    
            try:
                original_journal = response_OJDT.json()['OriginalJournal']
                lineas = response_OJDT.json()['JournalEntryLines']
                print(f"📄 Asiento OJDT {trans_id} encontrado. OriginalJournal = {original_journal}")
            except KeyError:
                print(f"❌ Asiento {trans_id} no tiene el formato esperado")
                continue
    
            # Función auxiliar para verificar depósito caja→banco
            def es_deposito_desde_caja_a_banco(lineas):
                linea_0 = next((l for l in lineas if l.get('Line_ID') == 0), None)
                linea_1 = next((l for l in lineas if l.get('Line_ID') == 1), None)
                return (
                    linea_0 and linea_1 and
                    linea_0.get('AccountCode') == '11010004' and
                    linea_1.get('AccountCode') == '11020004' and
                    (linea_0.get('Credit') or 0) > 0 and
                    (linea_1.get('Debit') or 0) > 0 and
                    linea_0.get('ContraAccount') == '11020004' and
                    linea_1.get('ContraAccount') == '11010004'
                )
    
            # Validaciones
            if original_journal != "ttDeposit":
                print("⚠️ No es un asiento basado en depósito (OriginalJournal != 'ttDeposit')")
                continue
            if not es_deposito_desde_caja_a_banco(lineas):
                print("⚠️ El asiento no representa un depósito de 11010004 → 11020004")
                continue
    
            # Paso 4.1: Extraer AdditionalReference
            linea_banco = next((l for l in lineas if l.get('AccountCode') == '11020004'), None)
            if not linea_banco:
                print("❌ No se encontró la línea de banco en el asiento")
                continue
    
            id_apertura_str = linea_banco.get('AdditionalReference')
            if not id_apertura_str:
                print("❌ No tiene AdditionalReference en la línea del banco")
                continue
    
            try:
                id_apertura_int = int(id_apertura_str, 0)
                print(f"🔢 idApertura encontrado: {id_apertura_int}")
            except ValueError:
                print(f"❌ AdditionalReference no convertible a int: {id_apertura_str}")
                continue
    
            if id_apertura_int < 150:
                print("⚠️ idApertura menor a 150, descartado")
                continue
    
            # Guardamos resultados
            lista_additional_reference_validos.append(id_apertura_int)
            lista_sub_diccionario_FintocId_OBNK_con_IdApertura.append(sub_diccionario_FintocId_OBNK_con_IdApertura)
            id_validos_en_fecha.append(id_apertura_int)
            print("✅ idApertura agregado a la lista\n")
    
        # Paso 5: Evaluar si esta fecha tuvo resultados
        if id_validos_en_fecha:
            mayor_apertura = max(id_validos_en_fecha)
            print("\n✅ Lista de idApertura válidos encontrados:")
            print(id_validos_en_fecha)
            print(f"🔑 El mayor idApertura de {fecha_actual.date()} es: {mayor_apertura}")
            break  # 💥 Detenemos el bucle, ya encontramos una fecha con resultados
        else:
            print(f"⚠️ No se encontraron idApertura válidos para {fecha_actual.date()}. Probando la siguiente fecha más reciente...\n")
            time.sleep(3)
    # Paso final (si ninguna fecha tuvo resultados)
    if not lista_additional_reference_validos:
        print("🚫 No se encontraron idApertura válidos en ninguna fecha del diccionario.")

    ###############################################################################
    # Ahora que ya tenemos el max idpertura = mayor_apertura vamos a ir a consutar
    # El endpoint b1s/v1/SEI_CIERRE para saber de qué fecha es la caja que justifica
    # el ultimo deposito 
    
    # configuramos la url 
    url_SEI_CIERRE = f"{os.getenv('BASE_URL_SAP')}/SEI_CIERRE?$filter=U_SEI_IDCJ eq {mayor_apertura}"
    
    # columnas a consultar: 
    columnas_SEI_CIERRE = {
        "$select" : "U_SEI_IDCJ,CreateDate,CreateTime"
        } 
    
    response_SEI_CIERRE = requests.get(url_SEI_CIERRE, headers = headers, verify = True)
    
    fecha_justifica_deposito = response_SEI_CIERRE.json()['value'][0]['CreateDate'][:10]
    OJDT_justifica_deposito = response_SEI_CIERRE.json()['value'][0]['U_SEI_OJDT']
    
    if OJDT_justifica_deposito == '':
        print(f"""
              El Cierre de Caja para idApertura: {mayor_apertura} aun no se ha hecho
              """)
    else: 
        print(f"""
    La ultima caja que se deposito fue la de la fecha: {fecha_justifica_deposito}
    El asiento es el: {OJDT_justifica_deposito}
    Su id de Apertura es el: {mayor_apertura}
    """)
    
    # La cantidad de dias que han transcurrido desde el ultimo deposito en efectivo: 
    cantidad_dias_desde_ultimo_deposito = (pd.to_datetime(dia_para_buscar_efectivo) - pd.to_datetime(fecha_justifica_deposito)).days

    print(f"""
    El ultimo deposito fue el {fecha_actual.strftime('%Y-%m-%d')} por: {df_OBNK[(df_OBNK['DueDate'] == fecha_actual.strftime('%Y-%m-%d')) & (df_OBNK['ExternalCode'] == (next(item['FintocId'] for item in lista_sub_diccionario_FintocId_OBNK_con_IdApertura if item['idApertura'] == str(mayor_apertura))))]["CreditAmount"].iloc[0].astype(int)}
    Correspondiente a la Caja del dia: {fecha_justifica_deposito}
    Han pasado {cantidad_dias_desde_ultimo_deposito} dias desde el ultimo deposito     
    """)
    
    
    ###########################################################################
    # A este punto ya tenemos fecha de la caja que justifica el ultimo deposito 
    # en SERVIAG, asique ahora queremos saber cuanto efectivo acumulado hay, es 
    # decir, cuanto Pagos Recibidos en Efectivo han habido entre el dia siguiente
    # a fecha_justifica_deposito y dia_para_buscar_efectivo
    print(f"""Vamos a ir a buscar Pagos recibidos en efectivo entre el {fecha_justifica_deposito} y el {dia_para_buscar_efectivo}""")    
    

    # configuramos la url_ORCT 
    url_ORCT = f"{os.getenv('BASE_URL_SAP')}/IncomingPayments"
    
    columnas_url_ORCT = {
        "$filter": f"CashSum ne 0 and DocDate ge '{fecha_justifica_deposito}' and DocDate le '{dia_para_buscar_efectivo}'", # Buscamos pagos recibidos en efectivo para el dia 'dia_para_buscar_efectivo'
        "$select": "CardCode,CardName,DocDate,CashSum,U_SEI_Code", # estas son las columnas que sacamos
        "$skip": 0
    }
    
    df_ORCT = pd.DataFrame()
    
    while True:
        print(f"Recuperando datos... Offset actual: {columnas_url_ORCT['$skip']}")  # Nueva línea para mostrar el progreso
    
        # Vamos a buscar los Pagos Recibidos en efectivo del dia 'dia_para_buscar_efectivo'
        response_ORCT = requests.get(url_ORCT, headers=headers, params=columnas_url_ORCT, verify=True)
    
        if response_ORCT.status_code not in range(200, 300): # en el range() entra cualquier numero entre 200 y 299 
           print(f"Error: status code: {response_ORCT.status_code}")
           sys.exit()
    
        data = response_ORCT.json()
        df_batch_ORCT = pd.DataFrame(data["value"], columns=["CardCode","CardName","DocDate","CashSum","U_SEI_Code"])
        
        if df_batch_ORCT.empty:
            print("No hay más registros para recuperar. Finalizando bucle.")  # Nueva línea para indicar que terminó
            break  # No hay más registros, salir del bucle
        
        # transformamos en df los pagos del intervalo de dias
        df_ORCT = pd.concat([df_ORCT, df_batch_ORCT], ignore_index=True)
        
        print(f"Registros obtenidos en esta iteración: {len(df_batch_ORCT)}")  # Nueva línea para ver cuántos registros se recuperaron
        
        columnas_url_ORCT["$skip"] += len(df_batch_ORCT)  # Preparar para saltar los registros ya recuperados
    
    # transformamos en df los pagos del intervalo de dias
    df_ORCT_pendiente_deposito = df_ORCT
    # transformamos DocDate a Fecha
    df_ORCT_pendiente_deposito['DocDate'] = pd.to_datetime(df_ORCT_pendiente_deposito['DocDate']).dt.strftime('%Y-%m-%d')
    # Aqui vamos a eliminar las filas que tengan la fecha del dia que ya se deposito
    df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito[df_ORCT_pendiente_deposito['DocDate'] != fecha_justifica_deposito]
    # Agrupar por fecha y sumar CashSum
    df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito.groupby(['DocDate','U_SEI_Code'], as_index=False)['CashSum'].sum()
    #Transformamos a CashSum en int
    df_ORCT_pendiente_deposito['CashSum'] = df_ORCT_pendiente_deposito['CashSum'].round(0).astype(int)
    df_ORCT_pendiente_deposito['DocDate'] = pd.to_datetime(df_ORCT_pendiente_deposito['DocDate']).dt.strftime('%d-%m-%Y')


    
    # Ordenar por fecha descendente
    df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito.sort_values(by='DocDate', ascending=False)
    
    #df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito.rename(columns ={"DocDate" :'Fecha', "CashSum": 'Total'})
    
    # df_ORCT_pendiente_deposito_html = df_ORCT_pendiente_deposito.to_html(index=False)

    
    # Este es el total de dinero que hay pendiente de depositar en SERVIPAG, CAJA VECINA, etc
    # este dinero está acumulado en la sucursal a la espera de deposito.
    dinero_pendiente_por_depositar_en_sucursal = int(df_ORCT_pendiente_deposito["CashSum"].sum())
    
    df_ORCT_pendiente_deposito_html = df_ORCT_pendiente_deposito.rename(columns = {"DocDate": 'Caja Fecha', "U_SEI_Code": 'Id Apertura', "CashSum": 'Total'}).to_html(index=False)

    # --- SIEMPRE pedir Cierre Tesorería (SIN pagos) ---
    id_apertura_para_cierre = None
    try:
        id_apertura_para_cierre = int(mayor_apertura)
    except Exception:
        id_apertura_para_cierre = None  # fallback si no se logró obtener
    
    bloque_cierre_tesoreria_html = (
        f"""
        <p style="margin:0 0 8px 0;"><b>Acerca del Cierre de Tesorería</b></p>
        <p style="margin:0;">
          <b>cpereira@lara.cl</b> podrias porfavor hacer el <b>Cierre de Tesorería</b> para el idApertura
          <b>{id_apertura_para_cierre}</b> correspondiente a la caja del día
          <b>{datetime.strptime(fecha_justifica_deposito, '%Y-%m-%d').strftime('%d-%m-%Y')}</b>.
        </p>
        <hr style="border:none;border-top:1px solid #ccc;margin:18px 0;">
        """
        if id_apertura_para_cierre is not None else
        """
        <p style="margin:0 0 8px 0;"><b>Solicitud de Cierre Tesorería</b></p>
        <p style="margin:0;">
          <b>cpereira@lara.cl</b> podrias porfavor hacer el <b>Cierre de Tesorería</b> para el idApertura
          <b>{id_apertura_para_cierre}</b> correspondiente a la caja del día
          <b>{datetime.strptime(fecha_justifica_deposito, '%Y-%m-%d').strftime('%d-%m-%Y')}</b>.
        </p>
        """
    )

    # ================== Envío de correo (caso SIN pagos en efectivo) ==================
    # load_dotenv(dotenv_path=r"C:\Users\pipel\OneDrive - Universidad Adolfo Ibanez\Desktop\Felipe\DALTO\Curso de PYTHON desde CERO (Completo)\Lara\Facturas rechazadas\.env") # en esta carpeta esta el .env
    usuario = os.getenv('mail_envio_control')
    clave = os.getenv('pw_control_caja_sucursal')
    servidor_smtp = os.getenv('sv_smtp')
    puerto_smtp = int(os.getenv('port_smtp'))
    # Agregamos los correos destinatarios
    To_para = ["yzambrano@lara.cl", "cpereira@lara.cl", "gguerra@lara.cl"]
    CC_con_copia = ["felipe@lara.cl"]
    Bcc_con_copia_oculta = ["cristian@lara.cl","feelipe.laral@gmail.com"]
    
    # --- Bloque URGENTE y CC automáticas si supera umbrales (monto o días) ---
    bloque_urgente_html = ""
    cond_monto = dinero_pendiente_por_depositar_en_sucursal > 200000
    cond_dias = (cantidad_dias_desde_ultimo_deposito is not None) and (cantidad_dias_desde_ultimo_deposito > 7)
    
    if cond_monto or cond_dias:
        razones = []
        if cond_monto:
            razones.append(
                f"un monto acumulado de CLP <b>{'{:,}'.format(dinero_pendiente_por_depositar_en_sucursal).replace(',', '.')}</b>"
            )
        if cond_dias:
            razones.append(
                f"<b>{cantidad_dias_desde_ultimo_deposito}</b> día(s) desde el último depósito"
            )
    
        bloque_urgente_html = f"""
          <p style="margin:10px 0;color:#b30000;">
            <b>URGENTE:</b> Se detecta {' y '.join(razones)}. Favor priorizar el depósito hoy mismo.
          </p>
        """
    
        # Añadir CC automáticas si se activa urgencia
        for extra_cc in ['jlsepulveda@lara.cl', 'gguerra@lara.cl', 'horacio@lara.cl']:
            if extra_cc not in CC_con_copia:
                CC_con_copia.append(extra_cc)
    # -------------------------------------------------------------------------
    
    todos_los_destinatarios = To_para + CC_con_copia + Bcc_con_copia_oculta
    print(f"Se va a enviar el correo a: {todos_los_destinatarios}")
    
    
    ###########################################################################
    # Logo LARA
    url_imagen_pie_de_firma = "https://www.lara.cl/wp-content/uploads/2023/09/azul-LARA.png"
    resp_imagen_pie_de_firma = requests.get(url_imagen_pie_de_firma, verify=True)
    imagen_bytes = resp_imagen_pie_de_firma.content
    print("Obtenida la imagen del logo LARA")
    
    ############################################################################
    
    # HTML de la tabla (ya lo tiene preparado)
    tabla_html = df_ORCT_pendiente_deposito_html
    
    # Fechas y asunto
    fecha_hora_actual = datetime.now().strftime("%d-%m-%Y %H:%M")
    dia_para_buscar_efectivo_formateado = datetime.strptime(dia_para_buscar_efectivo, '%Y-%m-%d').strftime('%d-%m-%Y')
    
    
    dia_para_buscar_pagos_en_efectivo_formateado = datetime.strptime(dia_para_buscar_efectivo, '%Y-%m-%d').strftime('%d-%m-%Y')

        
    asunto_correo_a_enviar = f"""Efectivo por depositar - Caja dia: {dia_para_buscar_pagos_en_efectivo_formateado}  Ex: Arqueo de caja+ Reporte de cierre Transbank"""
    
    # Monto del último depósito (ya calculado más arriba con df_OBNK y cada_bankmatch)
    # monto_ultimo_deposito = int(df_OBNK[
    #     (df_OBNK['DueDate'] == max(dict_df_OBNK_DueDate_datetime.keys()).strftime('%Y-%m-%d')) &
    #     (df_OBNK['BankMatch'] == cada_bankmatch)
    # ]['CreditAmount'].iloc[0])
    
    monto_ultimo_deposito = df_OBNK[(df_OBNK['DueDate'] == max(dict_df_OBNK_DueDate_datetime.keys()).strftime('%Y-%m-%d')) & (df_OBNK['ExternalCode'] == (next(item['FintocId'] for item in lista_sub_diccionario_FintocId_OBNK_con_IdApertura if item['idApertura'] == str(mayor_apertura))))]["CreditAmount"].iloc[0].astype(int)
    # Cuerpo en texto plano HTML 
    cuerpo_correo_a_enviar = f"""
    <html>
      <body>
        <p>
          Buenos días, esperando que se encuentren bien.<br><br>
    
          Les comento que el día <b>{dia_para_buscar_efectivo_formateado}</b> <b>no</b> se recibieron pagos en efectivo.<br><br>
        </p>
        {bloque_cierre_tesoreria_html}
        <p style="margin:0 0 10px 0;"><b>Respecto al efectivo acumulado para depósito</b></p>
        <p style="margin:0 0 8px 0;">
          La última fecha que se fue a depositar fue el día 
          <b>{fecha_actual.strftime('%d-%m-%Y')}</b>, por un monto de 
          <b>CLP {"{:,}".format(monto_ultimo_deposito).replace(",", ".")}</b>, correspondiente a la caja del día 
          <b>{datetime.strptime(fecha_justifica_deposito, '%Y-%m-%d').strftime('%d-%m-%Y')}</b>. 
          Han transcurrido <b>{cantidad_dias_desde_ultimo_deposito}</b> día(s) desde ese depósito.
        </p>
        <p style="margin:0 0 8px 0;">
          Actualmente hay un total de CLP <b>{"{:,}".format(dinero_pendiente_por_depositar_en_sucursal).replace(",", ".")}</b> 
          acumulado y pendiente por depositar. El detalle por día es:
        </p>
    
        {bloque_urgente_html}
    
        {tabla_html}
        <br><br>
    
    
        <p style="margin:12px 0 0 0;">
          yzambrano@lara.cl te agradecería que pudieras coordinar el depósito y, por favor, confirmar por este mismo medio. 
          Recuerda adjuntar el comprobante PDF firmado por quien realizará el depósito y copiar a 
          <b>cpereira@lara.cl</b> y <b>felipe@lara.cl</b>.
        </p>
    
        <p style="margin:14px 0 4px 0;">
          <img src="cid:logo_lara" width="300"><br>
          <small>Enviado el: {fecha_hora_actual}</small>
        </p>
      </body>
    </html>
    """
        
    # Construcción y envío
    msg = MIMEMultipart("related")
    msg['Subject'] = asunto_correo_a_enviar
    msg['From'] = usuario
    msg['To'] = ", ".join(To_para)
    msg['Cc'] = ", ".join(CC_con_copia)
    
    # Cuerpo HTML
    msg.attach(MIMEText(cuerpo_correo_a_enviar, "html"))
    
    # Logo inline
    imagen_mime = MIMEImage(imagen_bytes)
    imagen_mime.add_header('Content-ID', '<logo_lara>')
    msg.attach(imagen_mime)
    
    # Envío
    try:
        with smtplib.SMTP_SSL(servidor_smtp, puerto_smtp) as server:
            server.login(usuario, clave)
            server.sendmail(usuario, todos_los_destinatarios, msg.as_string())
        print("✅ Correo (sin ingresos) enviado correctamente con tabla y logo.")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")

# Luego de manejar el caso en el que no hayan pagos recibidos el dia de ayer, vamos a manejar el 
# caso más comun en el que si hay pagos en efectivo el dia de ayer. 
else:
    # Vamos a buscar los pagos en efectivo del dia anterior
    resultados = []
    for pago in response_ORCT.json()['value']:
        card_code = pago.get('CardCode', None)
        card_name = pago.get('CardName')
        doc_date = pago.get('DocDate', '')[:10]  # recorta la fecha: "2025-07-11T00:00:00Z" -> "2025-07-11"
        cash_sum = pago.get('CashSum')
        idApertura = pago.get('U_SEI_Code') # Sacamos el idApertura desde el asiento

        for rct2 in pago.get('PaymentInvoices', []): # estas son las lineas del detalle del pago recibido ORCT
            doc_entry = rct2.get('DocEntry')
            resultados.append({
                'CardCode': card_code,
                'CardName': card_name,
                'DocDate': doc_date,
                'CashSum': cash_sum,
                'DocEntry': doc_entry,
                'U_SEI_Code': idApertura
                
            })
            
    # vamos a crear un df con todos los pagos recibidos de 'dia_para_buscar_efectivo' 
    df_ORCT = pd.DataFrame(resultados)

    # Vamos a guardar el df idApertura de ORCT."U_SEI_Code"
    idApertura_ORCT = int(list(df_ORCT["U_SEI_Code"].unique())[0])
    
    # Total de efectivo recibido ese dia es: 
    total_recibido_dia_para_buscar_efectivo = int(df_ORCT["CashSum"].sum())

    print(f"""
    El df de los pagos recibidos el dia {dia_para_buscar_efectivo} 
    se ve asi: 
        
    {df_ORCT}
    
    El idApertura de ese dia es el: {idApertura_ORCT}
    El total ingresado en efectivo fue de: {total_recibido_dia_para_buscar_efectivo}

        """)
    
    ###########################################################################
    # Ahora vamos a buscar la apertura de caja de ese día 
    
    # configuramos la url_Apertura
    url_Apertura = f"{os.getenv('BASE_URL_SAP')}/Apertura({idApertura_ORCT})"
    columnas_Apertura = {
        "$select": "U_SEI_MNT" # el monto que se declaro en la apertura
        }
    response_Apertura = requests.get(url_Apertura, headers=headers, params=columnas_Apertura, verify=True)
    
    monto_Apertura = int(response_Apertura.json()['U_SEI_MNT']) # lo que declaro el cajero al abrir Caja
    
    ###########################################################################
    # A este punto tenemos total_recibido_dia_para_buscar_efectivo y monto_Apertura
    # la suma de ambos deberia ser a lo que se reporta en el cierre 
    # vamos a buscarla informacion del cierre. 
    
    #configuramos url_SEI_CIERRE 
    url_SEI_CIERRE = f"{os.getenv('BASE_URL_SAP')}/SEI_CIERRE?$filter=U_SEI_IDCJ eq {idApertura_ORCT}"
    
    columnas_SEI_CIERRE = {
        "$select": "U_SEI_EFCTV" # el monto al cierre es Efectivo apertura + efectivo recibido
        } 
    reponse_SEI_CIERRE = requests.get(url_SEI_CIERRE, headers=headers, params=columnas_SEI_CIERRE, verify=True)
    
    monto_CIERRE = int(reponse_SEI_CIERRE.json()['value'][0]['U_SEI_EFCTV']) # lo que declaro el cajero al abrir Caja
    print(f"""
          Efectivo declarado a en apertura: {monto_Apertura}
          Efectivo recibido durante el día: {total_recibido_dia_para_buscar_efectivo}
          Suma Apertura + Efectivo Recibio: {monto_Apertura+total_recibido_dia_para_buscar_efectivo} 
          Monto al cierre: {monto_CIERRE}
          """)
          

              
    # verificar que no haya diferencias
    if monto_Apertura+total_recibido_dia_para_buscar_efectivo != monto_CIERRE:
        # Existen diferencias. 
        # Si monto_CIERRE < monto_Apertura+total_recibido_dia_para_buscar_efectivo - > Falta plata por declarar
        # Si monto_CIERRE > monto_Apertura+total_recibido_dia_para_buscar_efectivo - > Conto plata demas en el cierre
        
        if monto_CIERRE > monto_Apertura+total_recibido_dia_para_buscar_efectivo:
            # 
            mail_diferencia = f"""Te comento que existe una diferencia positiva en el cierre de caja del dia {dia_para_buscar_efectivo} 
            es decir, se declararon CLP {monto_CIERRE}, que son: CLP {-1*(monto_Apertura+total_recibido_dia_para_buscar_efectivo-monto_CIERRE)} extras de los CLP {monto_Apertura+total_recibido_dia_para_buscar_efectivo} que deberian haber en la Caja
            """
            print(mail_diferencia)
        # falta plata                        
        elif monto_CIERRE < monto_Apertura+total_recibido_dia_para_buscar_efectivo: 
            mail_diferencia = f"""Te comento que existe una diferencia negativa en el cierre de caja del dia {dia_para_buscar_efectivo} 
            es decir, se declararon {monto_CIERRE}, que son: CLP {( monto_Apertura+total_recibido_dia_para_buscar_efectivo - monto_CIERRE)} menos de los CLP {monto_Apertura+total_recibido_dia_para_buscar_efectivo} que deberian haber en la Caja
            """
            print(mail_diferencia)
    else: 
        print("No hay diferencia en la caja")
        mail_diferencia = f"cpereira@lara.cl dado que no existe diferencia en el conteo de efectivo, podrias realizar el Cierre de Tesoreria para el idApertura: {idApertura_ORCT}" # si no hay diferencia este parrafo se deja vacio 
    
    
    
    
    ###########################################################################
    ###########################################################################
    ###### Comineza la busqueda pagos en efectivo recibidos acomulados ########
    # Ahora que ya sabemos que:
    # 1) Sí hay pagos en recibidos en efectivo para dia_para_buscar_efectivo
    # 2) Si hay o no hay error en el conteo de efectivo 
    
    # Vamos a ir a buscar la cantidad de efectivo que hay pendiente por depositar 
    print(f"\nEXISTEN Pagos Recibidos en efectivo - ORCT el día {dia_para_buscar_efectivo}\n")
    print(f"\nVamos a pasar a buscar el ultimo deposito en la cartola OBNK entre: {un_mes_atras} y {dia_para_buscar_efectivo}")
    url_OBNK = f"{os.getenv('BASE_URL_SAP')}/BankPages"

    columnas_url_OBNK = {
       "$filter": f"AccountCode eq '11020004' and DueDate ge '{un_mes_atras}' and DueDate le '{dia_para_buscar_efectivo}' and BankMatch gt 0", # Buscamos en la cuenta del Security, entre 'un_mes_atras' y 'dia_para_buscar_efectivo'
       "$select": "DueDate,BankMatch,Memo,DebitAmount,CreditAmount,Sequence,ExternalCode", # estas son las columnas que sacamos
       "$skip": 0
    }
    df_OBNK = pd.DataFrame() # creamos el df_OBNK vacio antes de comenzar el while

    while True:
        print(f"Recuperando datos... Offset actual: {columnas_url_OBNK['$skip']}")  # Nueva línea para mostrar el progreso

        response_OBNK = requests.get(url_OBNK, headers=headers, params=columnas_url_OBNK, verify=True)

        if response_OBNK.status_code not in range(200, 300): # en el range() entra cualquier numero entre 200 y 299 
           print(f"Error: status code: {response_OBNK.status_code}")
           sys.exit()

        data = response_OBNK.json()
        df_batch_OBNK = pd.DataFrame(data["value"], columns=["DueDate","BankMatch", "Memo","DebitAmount","CreditAmount","Sequence", "ExternalCode"])
        
        if df_batch_OBNK.empty:
            print("No hay más registros para recuperar. Finalizando bucle.")  # Nueva línea para indicar que terminó
            break  # No hay más registros, salir del bucle
        
        df_OBNK = pd.concat([df_OBNK, df_batch_OBNK], ignore_index=True)
        
        print(f"Registros obtenidos en esta iteración: {len(df_batch_OBNK)}")  # Nueva línea para ver cuántos registros se recuperaron
        
        columnas_url_OBNK["$skip"] += len(df_batch_OBNK)  # Preparar para saltar los registros ya recuperados
    # damos formato fecha a la columna "DueDate"
    df_OBNK['DueDate'] = pd.to_datetime(df_OBNK['DueDate']).dt.strftime('%Y-%m-%d')
    df_OBNK = df_OBNK.sort_values(by='DueDate',ascending=False)
    
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    
    print(f"""
    Los ultimos 30 días de depositos en 11020004 - Banco Security que ya estan reconciliados se ven asi
    {df_OBNK}
          
    """)
    
    # Transformarmos "DueDate" en key y "BankMatch" en value 
    dict_df_OBNK = df_OBNK.groupby('DueDate')['BankMatch'].apply(list).to_dict()
    
    print(f"""
    El diccionario con los numeros de reconciliacion de los ultimos depositos agrupados por dia se ven asi: 
          """)
    pprint(dict_df_OBNK)# ponemos el print pretty en otra linea





    # Transformamos las keys del dict_df_OBNK en datetime para poder quedarnos con la más reciente
    dict_df_OBNK_DueDate_datetime = {datetime.strptime(fecha, '%Y-%m-%d'): valores for fecha, valores in dict_df_OBNK.items()}
        
    # Paso 1: Obtener la key con fecha más reciente
    fecha_mas_reciente = max(dict_df_OBNK_DueDate_datetime.keys())
    
    # Paso 2: Obtener todos los BankMatch de esa fecha
    lista_bankmatch = dict_df_OBNK_DueDate_datetime[fecha_mas_reciente]
    
    # Inicializamos lista para guardar los idApertura
    lista_additional_reference_validos = []
    # Otro diccionario para llegar al FintocId de la cartola, desde el IdApertura. 
    lista_sub_diccionario_FintocId_OBNK_con_IdApertura = []
    
    # 🔹 Ordenar las fechas del diccionario de forma descendente (más reciente primero)
    fechas_ordenadas = sorted(dict_df_OBNK_DueDate_datetime.keys(), reverse=True)
    
    # 🔹 Inicializamos las listas finales (se mantendrán entre fechas)
    lista_additional_reference_validos = []
    lista_sub_diccionario_FintocId_OBNK_con_IdApertura = []
    
    # 🔁 Recorremos cada fecha hasta encontrar resultados válidos
    for fecha_actual in fechas_ordenadas:
        print(f"\n📅 Procesando fecha: {fecha_actual.date()}")
        lista_bankmatch = dict_df_OBNK_DueDate_datetime[fecha_actual]
    
        # Lista temporal por fecha (para saber si encontramos algo en esta fecha)
        id_validos_en_fecha = []
    
        # Paso 2.1 al 4 — Recorrer cada BankMatch
        for cada_bankmatch in lista_bankmatch:
            print(f"\n🔁 Procesando BankMatch: {cada_bankmatch}")
    
            # Paso 2.1 - Obtener TransactionNumber desde OMTH
            url_OMTH = f"{os.getenv('BASE_URL_SAP')}/ExternalReconciliationsService_GetReconciliation"
            payload_OMTH = {
                "ExternalReconciliationParams": {
                    "AccountCode": "11020004",
                    "ReconciliationNo": cada_bankmatch
                }
            }
            response_OMTH = requests.post(url_OMTH, json=payload_OMTH, headers=headers, verify=True)
    
            try:
                trans_id = response_OMTH.json()['ReconciliationJournalEntryLines'][0]['TransactionNumber']
                sub_diccionario_FintocId_OBNK_con_IdApertura = {
                    'idApertura' : response_OMTH.json()['ReconciliationJournalEntryLines'][0]['Ref3'],
                    'FintocId' : response_OMTH.json()['ReconciliationJournalEntryLines'][0]['Ref2']
                }
                print(f"✅ TransactionNumber encontrado: {trans_id}")
            except (KeyError, IndexError):
                print(f"❌ No se pudo obtener TransactionNumber para BankMatch {cada_bankmatch}")
                continue
    
            # Paso 3: Entrar al asiento contable (OJDT)
            url_OJDT = f"{os.getenv('BASE_URL_SAP')}/JournalEntries({trans_id})"
            params_OJDT = {"$select": "OriginalJournal,JournalEntryLines"}
            response_OJDT = requests.get(url_OJDT, headers=headers, params=params_OJDT, verify=True)
    
            try:
                original_journal = response_OJDT.json()['OriginalJournal']
                lineas = response_OJDT.json()['JournalEntryLines']
                print(f"📄 Asiento OJDT {trans_id} encontrado. OriginalJournal = {original_journal}")
            except KeyError:
                print(f"❌ Asiento {trans_id} no tiene el formato esperado")
                continue
    
            # Función auxiliar para verificar depósito caja→banco
            def es_deposito_desde_caja_a_banco(lineas):
                linea_0 = next((l for l in lineas if l.get('Line_ID') == 0), None)
                linea_1 = next((l for l in lineas if l.get('Line_ID') == 1), None)
                return (
                    linea_0 and linea_1 and
                    linea_0.get('AccountCode') == '11010004' and
                    linea_1.get('AccountCode') == '11020004' and
                    (linea_0.get('Credit') or 0) > 0 and
                    (linea_1.get('Debit') or 0) > 0 and
                    linea_0.get('ContraAccount') == '11020004' and
                    linea_1.get('ContraAccount') == '11010004'
                )
    
            # Validaciones
            if original_journal != "ttDeposit":
                print("⚠️ No es un asiento basado en depósito (OriginalJournal != 'ttDeposit')")
                continue
            if not es_deposito_desde_caja_a_banco(lineas):
                print("⚠️ El asiento no representa un depósito de 11010004 → 11020004")
                continue
    
            # Paso 4.1: Extraer AdditionalReference
            linea_banco = next((l for l in lineas if l.get('AccountCode') == '11020004'), None)
            if not linea_banco:
                print("❌ No se encontró la línea de banco en el asiento")
                continue
    
            id_apertura_str = linea_banco.get('AdditionalReference')
            if not id_apertura_str:
                print("❌ No tiene AdditionalReference en la línea del banco")
                continue
    
            try:
                id_apertura_int = int(id_apertura_str, 0)
                print(f"🔢 idApertura encontrado: {id_apertura_int}")
            except ValueError:
                print(f"❌ AdditionalReference no convertible a int: {id_apertura_str}")
                continue
    
            if id_apertura_int < 150:
                print("⚠️ idApertura menor a 150, descartado")
                continue
    
            # Guardamos resultados
            lista_additional_reference_validos.append(id_apertura_int)
            lista_sub_diccionario_FintocId_OBNK_con_IdApertura.append(sub_diccionario_FintocId_OBNK_con_IdApertura)
            id_validos_en_fecha.append(id_apertura_int)
            print("✅ idApertura agregado a la lista\n")
    
        # Paso 5: Evaluar si esta fecha tuvo resultados
        if id_validos_en_fecha:
            mayor_apertura = max(id_validos_en_fecha)
            print("\n✅ Lista de idApertura válidos encontrados:")
            print(id_validos_en_fecha)
            print(f"🔑 El mayor idApertura de {fecha_actual.date()} es: {mayor_apertura}")
            break  # 💥 Detenemos el bucle, ya encontramos una fecha con resultados
        else:
            print(f"⚠️ No se encontraron idApertura válidos para {fecha_actual.date()}. Probando la siguiente fecha más reciente...\n")
            time.sleep(3)
    # Paso final (si ninguna fecha tuvo resultados)
    if not lista_additional_reference_validos:
        print("🚫 No se encontraron idApertura válidos en ninguna fecha del diccionario.")

    ###############################################################################
    # Ahora que ya tenemos el max idpertura = mayor_apertura vamos a ir a consutar
    # El endpoint b1s/v1/SEI_CIERRE para saber de qué fecha es la caja que justifica
    # el ultimo deposito 
    
    # configuramos la url 
    url_SEI_CIERRE = f"{os.getenv('BASE_URL_SAP')}/SEI_CIERRE?$filter=U_SEI_IDCJ eq {mayor_apertura}"
    
    # columnas a consultar: 
    columnas_SEI_CIERRE = {
        "$select" : "U_SEI_IDCJ,CreateDate,CreateTime"
        } 
    
    response_SEI_CIERRE = requests.get(url_SEI_CIERRE, headers = headers, verify = True)
    
    fecha_justifica_deposito = response_SEI_CIERRE.json()['value'][0]['CreateDate'][:10]
    OJDT_justifica_deposito = response_SEI_CIERRE.json()['value'][0]['U_SEI_OJDT']
    
    if OJDT_justifica_deposito == '':
        print(f"""
              El Cierre de Caja para idApertura: {mayor_apertura} aun no se ha hecho
              """)
    else: 
        print(f"""
    La ultima caja que se deposito fue la de la fecha: {fecha_justifica_deposito}
    El asiento es el: {OJDT_justifica_deposito}
    Su id de Apertura es el: {mayor_apertura}
    """)
    
    # La cantidad de dias que han transcurrido desde el ultimo deposito en efectivo: 
    cantidad_dias_desde_ultimo_deposito = (pd.to_datetime(dia_para_buscar_efectivo) - pd.to_datetime(fecha_justifica_deposito)).days

    print(f"""
    El ultimo deposito fue el {fecha_actual.strftime('%Y-%m-%d')} por: {df_OBNK[(df_OBNK['DueDate'] == fecha_actual.strftime('%Y-%m-%d')) & (df_OBNK['ExternalCode'] == (next(item['FintocId'] for item in lista_sub_diccionario_FintocId_OBNK_con_IdApertura if item['idApertura'] == str(mayor_apertura))))]["CreditAmount"].iloc[0].astype(int)}
    Correspondiente a la Caja del dia: {fecha_justifica_deposito}
    Han pasado {cantidad_dias_desde_ultimo_deposito} dias desde el ultimo deposito     
    """)
    
    
    

    
    
    
    
    
    ###########################################################################
    # A este punto ya tenemos fecha de la caja que justifica el ultimo deposito 
    # en SERVIAG, asique ahora queremos saber cuanto efectivo acumulado hay, es 
    # decir, cuanto Pagos Recibidos en Efectivo han habido entre el dia siguiente
    # a fecha_justifica_deposito y dia_para_buscar_efectivo
    print(f"""Vamos a ir a buscar Pagos recibidos en efectivo entre el {fecha_justifica_deposito} y el {dia_para_buscar_efectivo}""")    
    
    # configuramos la url_ORCT 
    url_ORCT = f"{os.getenv('BASE_URL_SAP')}/IncomingPayments"

    columnas_url_ORCT = {
        "$filter": f"CashSum ne 0 and DocDate ge '{fecha_justifica_deposito}' and DocDate le '{dia_para_buscar_efectivo}'", # Buscamos pagos recibidos en efectivo para el dia 'dia_para_buscar_efectivo'
        "$select": "CardCode,CardName,DocDate,CashSum,U_SEI_Code" # estas son las columnas que sacamos
    }

    # Vamos a buscar los Pagos Recibidos en efectivo del dia 'dia_para_buscar_efectivo'
    response_ORCT = requests.get(url_ORCT, headers=headers, params=columnas_url_ORCT, verify=True)
    
    # transformamos en df los pagos del intervalo de dias
    df_ORCT_pendiente_deposito = pd.DataFrame(response_ORCT.json()['value'])
    # transformamos DocDate a Fecha
    df_ORCT_pendiente_deposito['DocDate'] = pd.to_datetime(df_ORCT_pendiente_deposito['DocDate']).dt.strftime('%Y-%m-%d')
    # Aqui vamos a eliminar las filas que tengan la fecha del dia que ya se deposito
    df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito[df_ORCT_pendiente_deposito['DocDate'] != fecha_justifica_deposito]
    # Agrupar por fecha y sumar CashSum
    df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito.groupby(['DocDate','U_SEI_Code'], as_index=False)['CashSum'].sum()
    #Transformamos a CashSum en int
    df_ORCT_pendiente_deposito['CashSum'] = df_ORCT_pendiente_deposito['CashSum'].round(0).astype(int)

    
    # Ordenar por fecha descendente
    df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito.sort_values(by='DocDate', ascending=False)
    
    #df_ORCT_pendiente_deposito = df_ORCT_pendiente_deposito.rename(columns ={"DocDate" :'Fecha', "CashSum": 'Total'})
    
    # df_ORCT_pendiente_deposito_html = df_ORCT_pendiente_deposito.to_html(index=False)

    
    # Este es el total de dinero que hay pendiente de depositar en SERVIPAG, CAJA VECINA, etc
    # este dinero está acumulado en la sucursal a la espera de deposito.
    dinero_pendiente_por_depositar_en_sucursal = int(df_ORCT_pendiente_deposito["CashSum"].sum())
    
    df_ORCT_pendiente_deposito_html = df_ORCT_pendiente_deposito.rename(columns = {"DocDate": 'Caja Fecha', "U_SEI_Code": 'Id Apertura', "CashSum": 'Total'}).to_html(index=False)

    mail = f"""
    Hola yzambrano@lara.cl esperando que estes bien. 
    
    Te comento que el día de ayer {datetime.strptime(dia_para_buscar_efectivo, '%Y-%m-%d').strftime('%d-%m-%Y')} no se recibieron pagos en efectivo. 
    

    Tambien te comento que la ultima fecha que se fue a depositar fue el dia {fecha_actual.strftime('%Y-%m-%d')} fue un deposito por: {df_OBNK[(df_OBNK['DueDate'] == fecha_actual.strftime('%Y-%m-%d')) & (df_OBNK['BankMatch'] == cada_bankmatch)]["CreditAmount"].iloc[0].astype(int)} dinero que correspondia a la caja del dia {datetime.strptime(fecha_justifica_deposito, '%Y-%m-%d').strftime('%d-%m-%Y')}
    Es decir han transcurrido: {cantidad_dias_desde_ultimo_deposito} dias sin depositar
    
    Actualmente hay un total de CLP {dinero_pendiente_por_depositar_en_sucursal} acumulado y pendiente por depositar
    
    Las cajas que hay pendientes de deposito son las siguientes: 
    {df_ORCT_pendiente_deposito}
    """
    
    ###### Fin busqueda pagos en efectivo recibidos acumulados ################
    ###########################################################################
    ###########################################################################
    

    
    
    
    
    acumula_filas_ORCT_efectivo = []

    # Ahora con el DocEntry que muestra la ORCT que corresponde a la OINV, vamos
    # a ir a buscar los detalles de la OINV
    for cada_docEntry_OINV in resultados: 
        
        #######################################################################
        # Esto es un parche asqueroso que pasa porque la persona transfirio una parte 
        # y la otra parte la dio en efectivo. Hay que arreglarlo a futuro 
        # Si el DocEntry es 57517, saltar esta iteración
        if cada_docEntry_OINV['DocEntry'] == 57517: 
            continue
        # hay que entrar a los pagos a nivel de linea
        #######################################################################    
        
        
        doc_entry_a_consultar = cada_docEntry_OINV['DocEntry'] # sacamos el DocEntry para ir a buscar cada OINV
        efectivo_ingresado = cada_docEntry_OINV['CashSum']
        # Configuramos la url de 
        url_OINV_individual = f"{os.getenv('BASE_URL_SAP')}/Invoices({doc_entry_a_consultar})"

        columnas_OINV_a_recuperar = { # Filtramos los campos de la OINV que nos interesan para adjuntar en el correo al cliente que rechazo el DTE
            "$select": "CardCode,CardName,FolioPrefixString,FolioNumber,CreationDate,DocTime,DocTotal,U_SEI_RED,U_SEI_VUEL,U_SEI_Code,UserSign,CreationDate,DocTime,FederalTaxID",
            "$skip": 0  # Inicialmente, no saltamos ningún registro. Son los primeros 20.
        }
        
        print(f"\n\nConsultando Factura DocEntry: {doc_entry_a_consultar}")
        response_folio_individual = requests.get(url_OINV_individual, headers=headers, params=columnas_OINV_a_recuperar, verify=True)

        response_folio_individual.json()
        # response_folio_individual.text
        
        cardCode = response_folio_individual.json()['CardCode']
        cardName = response_folio_individual.json()['CardName']
        folioNum = response_folio_individual.json()['FolioNumber']
        folioPref = response_folio_individual.json()['FolioPrefixString']
        docTotal = response_folio_individual.json()['DocTotal']
        redondeo = response_folio_individual.json()['U_SEI_RED']
        vuelto = response_folio_individual.json()['U_SEI_VUEL']
        efectivo_ingresado = efectivo_ingresado
        cajero = response_folio_individual.json()['UserSign']
        fechaFactura = response_folio_individual.json()['CreationDate'][:10]
        horaFactura = response_folio_individual.json()['DocTime']
        rut = response_folio_individual.json()['FederalTaxID']
        idCaja = response_folio_individual.json()['U_SEI_Code']
        
        
        print(f"""Informacion obtenida:
                      Folio: {folioNum}
                      Cliente: {cardName}
                      Total: {int(docTotal)}
                      Redondeo: {redondeo}
                      Vuelto: {vuelto}
                      Efectivo ingresado: {int(efectivo_ingresado)}
                      Cajero: {cajero}
                      Fecha:{fechaFactura}
                      Hora: {horaFactura}
                      idCaja: {idCaja}
                      """)
        
        # Bonus track, ahora vamos a hacer el get del los usuarios para sacar su email
        
        # Solo vamos a consultar el cajero si es distinto de Yani
        if cajero != 22:
            url_OUSR = f"{os.getenv('BASE_URL_SAP')}/Users({cajero})"
            columnas_OUSR = {
                "$select": "eMail,UserName",
                "$skip": 0  # Inicialmente, no saltamos ningún registro. Son los primeros 20.
                }
            request_OUSR = requests.get(url_OUSR, headers=headers, params=columnas_OUSR, verify=True)
                
            mail = request_OUSR.json()['eMail']
            nombreCajero = request_OUSR.json()['UserName']
        else:
            mail = 'yzambrano@lara.cl'
            nombreCajero = 'Yani Zambrano Guillen'
        
        ###########################################################################
        # A estas alturas vamos a armar el df con la informacion a enviar a los cajeros
        
        ORCT_efectivo_df = {
            'Rut' : rut,
            'Cliente': cardName,
            'Tipo DTE': folioPref,
            'Folio DTE': folioNum,
            'Total': int(docTotal),
            'Redondeo': int(redondeo),
            'Vuelto' : int(vuelto),
            'Ingreso': int(efectivo_ingresado),
            'Fecha' : fechaFactura,
            'Hora': horaFactura,
            'Ejecutivo': nombreCajero,
            'Mail': mail,
            'idCaja': idCaja
            }
        
        acumula_filas_ORCT_efectivo.append(ORCT_efectivo_df)


    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    ###############################################################################
    # Cuando ya terminamos de recolectar todos los pagos con efectivo recibidos, 
    # los juntamos en un df
    df_ORCT_enviable = pd.DataFrame(acumula_filas_ORCT_efectivo)

    df_ORCT_enviable.drop(columns=['Mail'], inplace=True) # eliminamos la columna mail

    # Vamos a obtener la suma del efectivo en caja: 
    ingresado_total_dia = df_ORCT_enviable['Ingreso'].sum()


    dia_para_buscar_pagos_en_efectivo_formateado = datetime.strptime(dia_para_buscar_efectivo, '%Y-%m-%d').strftime('%d-%m-%Y')
    
    
    ###########################################################################
    #### Ahora vamos a sacar la imagen con el logo de LARA desde la pagina de 
    #### LARA, vamos al wp-content
    url_imagen_pie_de_firma = "https://www.lara.cl/wp-content/uploads/2023/09/azul-LARA.png"
    resp_imagen_pie_de_firma = requests.get(url_imagen_pie_de_firma)
    imagen_bytes = resp_imagen_pie_de_firma.content
    
    print("Obtenida la imagen del logo LARA")
    
    
    ###########################################################################
    #### Ahora vamos a agregar las credenciales de correo para enviar el email
    #### servidor, puerto, usuario, contraseña, destinatarios
    
    usuario = os.getenv('mail_envio_control')
    clave = os.getenv('pw_control_caja_sucursal')
    servidor_smtp = os.getenv('sv_smtp')
    puerto_smtp = int(os.getenv('port_smtp'))
    # Agregamos los correos destinatarios
    To_para = [mail, "cpereira@lara.cl", "gguerra@lara.cl"]
    CC_con_copia = ["felipe@lara.cl"]
    Bcc_con_copia_oculta = ["cristian@lara.cl","feelipe.laral@gmail.com"]
    
    # --- Bloque URGENTE y CC automáticas si supera umbrales (monto o días) ---
    bloque_urgente_html = ""
    cond_monto = dinero_pendiente_por_depositar_en_sucursal > 200000
    cond_dias = (cantidad_dias_desde_ultimo_deposito is not None) and (cantidad_dias_desde_ultimo_deposito > 7)
    
    if cond_monto or cond_dias:
        razones = []
        if cond_monto:
            razones.append(
                f"un monto acumulado de CLP <b>{'{:,}'.format(dinero_pendiente_por_depositar_en_sucursal).replace(',', '.')}</b>"
            )
        if cond_dias:
            razones.append(
                f"<b>{cantidad_dias_desde_ultimo_deposito}</b> día(s) desde el último depósito"
            )
    
        bloque_urgente_html = f"""
          <p style="margin:10px 0;color:#b30000;">
            <b>URGENTE:</b> Se detecta {' y '.join(razones)}. Favor priorizar el depósito hoy mismo.
          </p>
        """
    
        # Añadir CC automáticas si se activa urgencia
        for extra_cc in ['jlsepulveda@lara.cl', 'gguerra@lara.cl', 'horacio@lara.cl']:
            if extra_cc not in CC_con_copia:
                CC_con_copia.append(extra_cc)
    # -------------------------------------------------------------------------
    
    todos_los_destinatarios = To_para + CC_con_copia + Bcc_con_copia_oculta
    print(f"Se va a enviar el correo a: {todos_los_destinatarios}")
    
    
    
    
    # Transformamos el df a HTML para adjuntarlo en el correo 
    tabla_html = df_ORCT_enviable.to_html(index=False)
    
    # obtenemos la fecha y hora actual para adjuntarla al correo que vamos a enviar
    fecha_hora_actual = datetime.now().strftime("%d-%m-%Y %H:%M")
    
    
    asunto_correo_a_enviar = f"""Efectivo por depositar - Caja dia: {dia_para_buscar_pagos_en_efectivo_formateado}  Ex: Arqueo de caja+ Reporte de cierre Transbank"""
    
    
    # Bloque opcional: párrafo sobre diferencias en caja (si aplica)
    bloque_diferencia_html = ""
    if mail_diferencia:  # mail_diferencia es '' cuando no hay diferencia
        bloque_diferencia_html = f"""
        <hr style="border:none;border-top:1px solid #ccc;margin:18px 0;">
        <p style="margin:0 0 8px 0;"><b>Revisión de diferencias en caja</b></p>
        <p style="margin:0;">
            {mail_diferencia.replace('\n', '<br>')}
        </p>"""

    
    
    
    cuerpo_correo_a_enviar = f"""
    <html>
      <body>
    <p>
      Hola {mail}, esperando que estés bien.<br><br>
    
      Te comento que los pagos en efectivo recibidos el día <b>{dia_para_buscar_pagos_en_efectivo_formateado}</b> 
      suman un total de CLP <b>{"{:,}".format(ingresado_total_dia).replace(",", ".")}</b>.<br><br>
    
      Según el detalle de la siguiente tabla:<br><br>
      {tabla_html}<br><br>
      
      {bloque_diferencia_html}
    

      <!-- Contexto de depósitos y efectivo pendiente -->
      <hr style="border:none;border-top:1px solid #ccc;margin:18px 0;">
      <p style="margin:0 0 10px 0;"><b>Respecto al efectivo acumulado para deposito</b></p>
      <p style="margin:0 0 8px 0;">
        La última fecha que se fue a depositar fue el día 
        <b>{fecha_actual.strftime('%Y-%m-%d')}</b>, por un monto de 
        <b>CLP {"{:,}".format(int(df_OBNK[
            (df_OBNK['DueDate'] == fecha_actual.strftime('%Y-%m-%d')) & 
            (df_OBNK['BankMatch'] == cada_bankmatch)
        ]['CreditAmount'].iloc[0])).replace(",", ".")}</b>, correspondiente a la caja del día 
        <b>{datetime.strptime(fecha_justifica_deposito, '%Y-%m-%d').strftime('%d-%m-%Y')}</b>. 
        Han transcurrido <b>{cantidad_dias_desde_ultimo_deposito}</b> día(s) desde ese depósito.
      </p>
      <p style="margin:0 0 8px 0;">
        Actualmente hay un total de CLP <b>{"{:,}".format(dinero_pendiente_por_depositar_en_sucursal).replace(",", ".")}</b> acumulado y pendiente por depositar. 
        El detalle por día es:
      </p>
      {df_ORCT_pendiente_deposito_html}<br><br>
    
      Te agradecería que pudieras enviar el arqueo de caja correspondiente para confirmar los montos y, si todo está correcto, 
      preparar el sobre con el voucher para realizar el depósito.<br><br>
    
      Además, recuerda que una vez entregado el dinero en efectivo para el depósito, debes informarlo por correo a 
      <b>cpereira@lara.cl</b> y a mí, <b>felipe@lara.cl</b>. 
      Junto con el correo, debes adjuntar en PDF el documento que acredite la entrega del efectivo para depósito al portador, 
      firmado por quien lo va a depositar.<br><br>
    
      <img src="cid:logo_lara" width="300"><br>
      <small>Enviado el: {fecha_hora_actual}</small>
    </p>
      </body>
    </html>
    """
    
    
    
    
    # Prinero agregamos el asunto del mensaje, cuerpo del mensaje el To (para)
    # From (emisor del correo), Cc (con copia)
    msg = MIMEMultipart("related")
    msg['Subject'] = asunto_correo_a_enviar
    msg['From'] = usuario
    msg['To'] = ", ".join(To_para)
    msg['Cc'] = ", ".join(CC_con_copia)
    
    
    # Cuerpo HTML
    msg.attach(MIMEText(cuerpo_correo_a_enviar, "html"))
    
    # Adjuntar imagen del logo
    imagen_mime = MIMEImage(imagen_bytes)
    imagen_mime.add_header('Content-ID', '<logo_lara>')
    msg.attach(imagen_mime)
    
    # 💡 5) Enviar correo
    try:
        with smtplib.SMTP_SSL(servidor_smtp, puerto_smtp) as server:
            server.login(usuario, clave)
            server.sendmail(usuario, todos_los_destinatarios, msg.as_string())
        print(f"✅ Correo enviado correctamente a {todos_los_destinatarios} con tabla y logo.")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")