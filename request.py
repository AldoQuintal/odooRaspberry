import psycopg2
import datetime
import requests
import logging
import json

#  Configurar la conexion a PostgresSQL
PSQL_HOST = "10.10.2.246"
PSQL_PORT = "5432"
PSQL_USER = "odoo"
PSQL_PASS = "unkkuri-secret-pw"
PSQL_DB   = "odoo"

api_inv = 'http://localhost:8000/api/inventarios'
api_entr = 'http://localhost:8000/api/entregas'

_LOGGER = logging.getLogger(__name__)

def FSM_Core():
    
    ProcesaInventario()
    ProcesaEntrega()
    


def ProcesaInventario():
    print("###### Procesa Inventarios ######")

    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB)
        conn = psycopg2.connect(connstr)
        cur = conn.cursor()

        response = requests.get(url=api_inv,auth=None,verify=False)
        tanques = response.json()
        print(f'json_response: {tanques}')

        query="SELECT vr_tanque FROM gsm_tanques ORDER BY vr_tanque ASC"
        cur.execute(query)
        vr_tanks = cur.fetchall()
        print(f'Vr_tanks : {vr_tanks}')

        if tanques and vr_tanks:
            for b in tanques:
                for a in vr_tanks:
                    print(f'*** {a}')
                    print(f'... {b["vr_tanque"]}')
                    
                    if b["vr_tanque"] == a[0]:
                        query = f"""UPDATE gsm_tanques SET vr_volumen = '{b["vr_volumen"]}', vr_vol_ct= '{b["vr_vol_ct"]}', vr_agua='{b["vr_agua"]}', vr_temp='{b["vr_temp"]}' WHERE vr_tanque='{b["vr_tanque"]}'"""
                        cur.execute(query)
                        conn.commit()

                        query = f"""SELECT entr_hoy FROM gsm_tanques WHERE vr_tanque = '{b["vr_tanque"]}'"""
                        print(query)
                        cur.execute(query)
                        hora_validar = cur.fetchone()
                        print(f'hora_validar: {hora_validar}')

                        if hora_validar[0]:
                            hora = int(hora_validar[0])
                        else:
                            hora = 0
                        
                        print(f'Hora: {hora}')
                        hora_actual = datetime.datetime.now().hour
                        print(f'Hora actual: {hora_actual}')
                        if int(hora_actual) == hora:
                            query = f"""UPDATE gsm_tanques SET entr_hoy = '{hora_actual}' WHERE vr_tanque = '{b["vr_tanque"]}'"""
                            cur.execute(query)
                            conn.commit()
                            print("Procedo a crear una existencia .................................")
                            ## Recuperando valores para insertar la existencia
                            # RFC
                            query = "SELECT rfc, clave_siic FROM res_company LIMIT 1"
                            cur.execute(query)
                            datos = cur.fetchone()
                            if datos[0] != None:
                                rfc = datos[0]
                            else:
                                rfc = ""
                                
                            
                            if datos[1] != None:
                                siic = datos[1] 
                            else:
                                siic = ""
                                
                                
                            # Clave producto 
                            query = f"SELECT clv_prd, vol_fond, clave_tanque FROM gsm_tanques WHERE vr_tanque = '{b['vr_tanque']}' "
                            cur.execute(query)
                            datos = cur.fetchone()
                            
                            if datos[0] != None:
                                clave_prd = datos[0]
                            else:
                                clave_prd = ""
                                
                            if datos[1] != None:
                                vol_fond = datos[1]
                            else:
                                vol_fond = ""
                            
                            if datos[2] != None:
                                clave_tanque = datos[2]
                            else:
                                clave_tanque = ""

                            # Vol Util
                            vol_util = float(b['vr_volumen']) - float(vol_fond)

                            print("vol_extr")

                            vol_extr = 0.0 + float(b['vol_ant']) - float(b['vr_volumen'])
                            
                            print(vol_extr)
                            if vol_extr < 0.0:
                                vol_extr = vol_extr * (-1)
                            
                            volumen_extr = "{:012.3f}".format(vol_extr)
                            

                            # Recupera turno
                            query = "SELECT id as turno_actual FROM gsm_turnos where estado='A' order by id desc"
                            cur.execute(query)
                            datos = cur.fetchone()
                            if datos[0] != None:
                                turno = datos[0]
                            else:
                                turno = ""
                            
                            now = datetime.datetime.now()
                            fecha = now.strftime("%Y%m%d%H%M")


                            #Valores a insertar 
                            print(f'Create date: {datetime.datetime.now()}')
                            print(f'RFC: {rfc}')
                            print(f'Clave siic: {siic}')
                            print(f'Tanque: {b["vr_tanque"]}')
                            print(f'Clave producto: {clave_prd}')
                            print(f'Vol_util : {vol_util}')
                            print(f'Vol_fond: {vol_fond}')
                            print(f'Vol_agua: {b["vr_agua"]}')
                            print(f'Vol_dispon: {b["vr_volumen"]}')
                            #print(f'Vol_extr: {}')
                            print(f'Vol_recep: 0.00')
                            print(f'Temp: {b["vr_temp"]}')
                            print(f'Med_ant: Medición anterior?')
                            print(f'med_act: fecha?')
                            print(f'Pendiente = 1')
                            print(f'Fecha: {datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")}')
                            print(f'Turno: {turno}')
                            print(f'Vol_ct: {b["vr_vol_ct"]}')
                            print(f'Clave_tanque: {clave_tanque}')
                            print(f'Volumen_extra: {volumen_extr}')
                            print(f'Fecha: {fecha}')

                            query = f"""INSERT INTO gsm_existencias (create_date, write_date, rfc,clave,tanque,clv_prd,vol_util,vol_fond,vol_agua,vol_dispon,vol_extr,vol_recep,temp,med_ant,med_act,fecha,turno,vol_ct,clave_tanque) VALUES 
                            ('{datetime.datetime.now()}', '{datetime.datetime.now()}', '{rfc}', '{siic}', '{b["vr_tanque"]}', '{clave_prd}', '{vol_util}', '{vol_fond}', '{b["vr_agua"]}', '{b["vr_volumen"]}', '{volumen_extr}', '0.00', '{b["vr_temp"]}', '', '', '{datetime.datetime.now().strftime("%Y%m%d%H%M")}', '{turno}', '{b["vr_vol_ct"]}', '{clave_tanque}' )"""
                            print(query)
                            cur.execute(query)
                            conn.commit()
                        

        conn.close()

    except Exception as ex:
        print("Error")
        conn.close()
        _LOGGER.info(str(ex))

def ProcesaEntrega():
    print("###### Procesa Entregas ######")
    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PSQL_HOST, PSQL_PORT, PSQL_USER, PSQL_PASS, PSQL_DB)
        conn = psycopg2.connect(connstr)
        cur = conn.cursor()

        response = requests.get(url=api_entr,auth=None,verify=False)
        entregas = response.json()
        
        for entrega in entregas:
            print(entrega)
            query=f"""SELECT * FROM gsm_entregas WHERE vr_tanque = '{entrega['vr_tanque']}' and
            fecha_ini = '{entrega['fecha_ini']}' and
            fecha_fin = '{entrega['fecha_fin']}' and
            aum_neto = '{entrega['aum_neto']}' and
            aum_bruto = '{entrega['aum_bruto']}' """
            cur.execute(query)
            print(query)
            entrega_valida = cur.fetchall() 
            print(f'valida_entrega: {entrega_valida}')
            if not entrega_valida:

                query = "SELECT clave_medicion FROM gsm_medicion WHERE tanque_id = {} and clave_medicion LIKE 'SMD%'".format(entrega['vr_tanque'])
                cur.execute(query)
                clave_smd = cur.fetchone()
                print(f'Clave_smd: {clave_smd}')

                #Se recupera la clave_medicion SME
                query = "SELECT clave_medicion FROM gsm_medicion WHERE tanque_id = {} and clave_medicion LIKE 'SME%'".format(entrega['vr_tanque'])
                cur.execute(query)
                clave_sme = cur.fetchone()
                print(f'Clave_sme: {clave_sme}')

                #Se obtiene la clave de indentificacion partiendo de la clave_producto
                query = "SELECT clave_identificacion FROM gsm_combustibles WHERE clave_producto = (SELECT clv_prd FROM gsm_tanques WHERE vr_tanque = '{}')".format(entrega['vr_tanque'])
                cur.execute(query)
                clv_iden = cur.fetchone()
                print(f'clv_iden: {clv_iden}')

                #Se obtiene la clave de tanque
                query = "SELECT clave_tanque, combustible_id, vr_codigo FROM gsm_tanques WHERE vr_tanque = '{0}'".format(entrega['vr_tanque'])
                cur.execute(query)
                clave_tanque = cur.fetchone()
                print(f'Clave_tanque: {clave_tanque}')

                # Se obtiene el RFC
                query = "SELECT rfc, clave_siic FROM res_company LIMIT 1"
                cur.execute(query)
                rfc = cur.fetchone()

                #Formateo de fecha Inicial y final 
                fecha_ini =  datetime.datetime.strptime(entrega["fecha_ini"] , "%Y/%m/%d %H:%M:%S")
                fecha_ini_format = datetime.datetime.strftime(fecha_ini, "%y%m%d%H%M")

                fecha_fin = datetime.datetime.strptime(entrega["fecha_fin"], "%Y/%m/%d %H:%M:%S")
                fecha_fin_format = datetime.datetime.strftime(fecha_fin, "%y%m%d%H%M")
                

                query = f"""INSERT INTO gsm_entregas (vr_tanque, fecha_ini, fecha_fin, vol_ini, vol_fin, vol_ct_ini, vol_ct_fin, agua_ini, agua_fin, temp_ini, temp_fin, aum_bruto, aum_neto, clv_prd, clave_medicion_sme, tipo_combustible, clave_medicion_smd, clave_tanque, rfc, combustible_id, vr_codigo)
                VALUES ('{entrega['vr_tanque']}', '{fecha_ini_format}', '{fecha_fin_format}', '{entrega['vol_ini']}', '{entrega['vol_fin']}', '{entrega['vol_ct_ini']}', '{entrega['vol_ct_fin']}', '{entrega['agua_ini']}', '{entrega['agua_fin']}', '{entrega['temp_ini']}', '{entrega['temp_fin']}', '{entrega['aum_neto']}', '{entrega['aum_bruto']}', '{entrega['clv_prd']}', '{clave_sme[0]}', '{clv_iden[0]}', '{clave_smd[0]}', '{clave_tanque[0]}', '{rfc[0]}', '{clave_tanque[1]}', '{clave_tanque[2]}')"""
                print(query)
                cur.execute(query)
                conn.commit()

    except Exception as ex:
        print("Error Entregas")
        conn.close()
        _LOGGER.info(str(ex))
    




if __name__ == "__main__":
    FSM_Core()
    
