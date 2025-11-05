import psycopg2
import datetime
import pymssql
import sys

# def koneksi_ora():
#     try:
#         return cx_Oracle.connect("information ora database credential in here")
#     except Exception as e:
#         print(f'Terjadi kesalahan koneksi ORA : {e}')
#         sys.exit(1)

def koneksi_mssql(ip,password):
    try:
        return pymssql.connect("information mysql database credential in here")
    except Exception as e:
        print(f'Terjadi kesalahan koneksi sql server : {e}')
        sys.exit(1)

def koneksi_postgresql(ip):
    try:
        return psycopg2.connect("information postgresql database credential in here")
    except Exception as e:
        print(f'Terjadi kesalahan koneksi postgreSQL : {e}')
        sys.exit(1)

# ubah range nya menjadi 3 hari saja info pak setyawan
# def ambil_absen():
#     # sql = """select absen.* from absen, EMP 
#     #         where absen.JDE_NO = emp.JDE_NO and emp.ADMIN_CODE = 'MOD' and emp.STATPAY = 'OPR' and 
#     #         TRUNC(absen.TANGGAL) BETWEEN TRUNC(SYSDATE - 3) and TRUNC(SYSDATE)"""
#     sql = """select absen.* from absen, EMP 
#                 where absen.JDE_NO = emp.JDE_NO and (
#                     (emp.ADMIN_CODE = 'MOD' and emp.STATPAY = 'OPR')
#                     or emp.department = 'IT')
#                 and TRUNC(absen.TANGGAL) BETWEEN TRUNC(SYSDATE - 3) and TRUNC(SYSDATE)"""
#     cur_ora.execute(sql)
#     data_absen_ora = cur_ora.fetchall()
#     kol_absen_ora = [kol[0] for kol in cur_ora.description]
#     for row in data_absen_ora:
#         insert_absen = f"""insert into t_absen ({','.join(kol_absen_ora)}, download_date)
#                         values ({','.join(['%s' for _ in kol_absen_ora])}, %s) on conflict (jde_no, tanggal) do update set
#                         {','.join([f"{col} = excluded.{col}" for col in kol_absen_ora])}, download_date = excluded.download_date"""
#         cur_psql.execute(insert_absen, (*row, tanggal))
#         print(row[0], row[1], row[2], row[3], row[4], tanggal)
#     conn.commit()

# def ambil_eventlog():
#     sql = """select a.EVENT_DATETIME, a.EVENT_DATE, a.EVENT_TIME, a.EVENT_ID, a.SYSTEM_DATETIME, a.USER_ID, a.NETWORK_ID,
#                 a.CONTROLLER_ID, a.MODULE_ID, a.UNIT_ADDR, a.FUNC_KEY, a.OPERATION_MODE, a.DOOR_INOUT, a.DOOR_STATE
#             from EVENTLOG_TBL a, EMP
#             where a.user_id = emp.JDE_NO and (
#                 (emp.ADMIN_CODE = 'MOD' and emp.STATPAY = 'OPR')
#                 or emp.department = 'IT')
#             and TO_DATE(a.event_date, 'YYYYMMDD') BETWEEN TRUNC(SYSDATE - 2) and TRUNC(SYSDATE)"""
#     cur_ora.execute(sql)
#     data_event_ora = cur_ora.fetchall()
#     kol_event_ora = [kol[0] for kol in cur_ora.description]
#     for row in data_event_ora:
#         # mshift = shift(datetime.datetime.strptime(fix_time_format(row[2]), "%H:%M:%S"))
#         insert_event = f"""insert into t_eventlog ({','.join(kol_event_ora)}, download_date)
#                         values ({','.join(['%s' for _ in kol_event_ora])}, %s) on conflict (event_datetime, user_id, controller_id, system_datetime)
#                         do update set
#                         {','.join([f"{col} = excluded.{col}" for col in kol_event_ora])}, download_date = excluded.download_date"""
#         cur_psql.execute(insert_event, (*row, tanggal))
#         print(row[1], row[2], row[3], row[5])
#     conn.commit()

# transfer data dari 241.28 ke 241.9 -> dikembalikan menjadi dari 241.8 ke 241.9
def ambil_event_face_to_svr():
    sql = """select event_datetime, event_date, event_time, user_id from eventlog_tbl_faceid_acp
                where CAST(event_date AS DATE) BETWEEN CAST(GETDATE() - 2 AS DATE) AND CAST(GETDATE() AS DATE)"""
    # device_name in ('DH-ACP-12 IN MOD 1', 'DH-ACP-05 IN MOD 2')
                # and 
    cur_sql_lineup.execute(sql)
    data_event_sql = cur_sql_lineup.fetchall()
    for row in data_event_sql:
        insert_sql = """insert into t_eventlog2 (event_datetime, event_date, event_time, event_id, system_datetime, user_id, network_id,
                                    controller_id, module_id, unit_addr, func_key, operation_mode, door_inout, door_state, download_date)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            on conflict (event_datetime, user_id, controller_id, system_datetime) do nothing"""
        cur_psql.execute(insert_sql, (row[0], row[1], row[2], '93', row[0], row[3], 93, 93, 93, '93', 93, 93, 93, 93, tanggal))
        print(row[0], row[1], row[2], row[3], tanggal, 'to 241.9')
    conn.commit()

# transfer data dari 241.28 ke 241.8
def ambil_event_todb_face():
    sql = "select * from eventlog_tbl_faceid_acp where CAST(event_date AS DATE) BETWEEN CAST(GETDATE() - 7 AS DATE) AND CAST(GETDATE() AS DATE) and user_id = '1769'"
    cur_sql_lineup.execute(sql)
    event_face_sql = cur_sql_lineup.fetchall()
    kol_event_face = [kol[0] for kol in cur_sql_lineup.description]
    for row in event_face_sql:
        insert_sql = f"""IF NOT EXISTS (SELECT 1 FROM eventlog_tbl_faceid_acp WHERE user_id = '{row[0]}' and event_datetime = '{row[1]}') 
                        BEGIN INSERT INTO eventlog_tbl_faceid_acp ({','.join(kol_event_face)})
                              VALUES ({','.join(['%s' for _ in kol_event_face])}); 
                        END"""
        cur_sql.execute(insert_sql, (*row,))
        print(row[0], row[1], row[2], row[3], tanggal, 'to 241.8')
    con_sql.commit()

try:
    waktu_awal = datetime.datetime.now()

    conn = koneksi_postgresql(ip='')
    print('Opened database Postgresql Succesfully')
    cur_psql = conn.cursor()
    cur_psql.execute("SET statement_timeout = '15000ms'")

    # con_ora = koneksi_ora()
    # cur_ora = con_ora.cursor()

    con_sql = koneksi_mssql(ip='', password='')
    cur_sql = con_sql.cursor()

    con_sql_lineup = koneksi_mssql(ip='', password='')
    cur_sql_lineup = con_sql_lineup.cursor()

    tanggal = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ambil_absen()
    # ambil_eventlog()

    # ambil_event_face_to_svr()   # 1. get data from db face 241.28 then store to db 241.9
    ambil_event_todb_face()     # 2. get data from db face 241.28 then store to db 241.8

    waktu_akhir = datetime.datetime.now()
    print(f"Waktu Eksekusi : {waktu_akhir - waktu_awal}")
except Exception as e:
    print(f'Error eksekusi : {e}')
    sys.exit(1)