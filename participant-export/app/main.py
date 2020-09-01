from flask import Flask
from flask import request

import flask_excel as excel
import pymysql
import os
from datetime import date, timezone
import pytz

app = Flask(__name__)
excel.init_excel(app)

mysqldb = pymysql.connect(
    host=os.environ['DB_HOST'],
    user=os.environ['DB_USERNAME'],
    passwd=os.environ['DB_PASSWORD'],
    database=os.environ['DB_DATABASE'],
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

mycursor = mysqldb.cursor()


def utc_to_local(utc_dt):
    local_tz = pytz.timezone('Asia/Jakarta')
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=local_tz)


def get_event(rdt_event_id):
    sql = ("SELECT * FROM rdt_events WHERE id = %s")
    val = (rdt_event_id,)
    mycursor.execute(sql, val)

    return mycursor.fetchone()


def calculate_age(birth_date):
    today = date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))


@app.route('/', methods=['GET'])
def index():
    mysqldb.ping(reconnect=True)
    return {
        "status": "OK"
    }


@app.route('/export', methods=['GET'])
def export():
    rdt_event_id = request.args.get('rdt_event_id')

    event = get_event(rdt_event_id)

    sql = "SELECT a.*, b.*, c.name as city_name, d.name as district_name, e.name as village_name," \
          "b.created_at as registered_datetime " \
          "FROM rdt_invitations a " \
          "JOIN rdt_applicants b ON a.rdt_applicant_id = b.id " \
          "JOIN areas c ON b.city_code = c.code_kemendagri " \
          "JOIN areas d ON b.district_code = d.code_kemendagri " \
          "JOIN areas e ON b.village_code = e.code_kemendagri " \
          "WHERE a.rdt_event_id = %s"
    mycursor.execute(sql, (rdt_event_id,))

    rows = [
        ['NOMOR PESERTA', 'ID EVENT', 'ID KLOTER', 'NAMA KEGIATAN', 'PENYELENGGARA', 'NIK', 'NAMA', 'NOMOR TELEPON',
         'JENIS KELAMIN', 'TANGGAL LAHIR', 'UMUR (TAHUN)', 'ALAMAT DOMISILI', 'KAB/KOTA DOMISILI', 'KODE KAB/KOTA',
         'KECAMATAN', 'KODE KECAMATAN', 'KELURAHAN/DESA', 'KODE KELURAHAN/DESA', 'PNS',
         'JENIS PEKERJAAN', 'NAMA PEKERJAAN', 'NAMA TEMPAT BEKERJA', 'GEJALA', 'CATATAN GEJALA', 'RIWAYAT KONTAK',
         'RIWAYAT KEGIATAN', 'STATUS KESEHATAN', 'TANGGAL PENDAFTARAN', 'KIRIM UNDANGAN', 'LOKASI CHECKIN',
         'CHECKIN KEHADIRAN', 'KODE SAMPEL LAB', 'TANGGAL HASIL LAB', 'HASIL TEST', 'KIRIM HASIL TEST']
    ]

    for record in mycursor.fetchall():
        age = None

        birth_date = record['birth_date']
        if birth_date is not None:
            age = calculate_age(birth_date)

        register_datetime_utc = record['registered_datetime']
        register_datetime_local = utc_to_local(register_datetime_utc)

        # Attended At
        attended_datetime_utc = record['attended_at']
        attended_datetime_local = None

        if attended_datetime_utc is not None:
            attended_datetime_local = utc_to_local(attended_datetime_utc)

        # Result
        result_at_datetime_utc = record['result_at']
        result_at_datetime_local = None

        if result_at_datetime_utc is not None:
            result_at_datetime_local = utc_to_local(result_at_datetime_utc)

        # Notified
        notified_datetime_utc = record['notified_at']
        notified_datetime_local = None

        if notified_datetime_utc is not None:
            notified_datetime_local = utc_to_local(notified_datetime_utc)

        # Notified Result
        notified_result_datetime_utc = record['notified_result_at']
        notified_result_datetime_local = None

        if notified_result_datetime_utc is not None:
            notified_result_datetime_local = utc_to_local(notified_result_datetime_utc)

        rows.append([
            record['registration_code'],
            record['rdt_event_id'],
            record['rdt_event_schedule_id'],
            event['event_name'].upper(),
            event['host_name'].upper(),
            record['nik'],
            record['name'].upper(),
            record['phone_number'],
            record['gender'],
            birth_date.strftime("%Y-%m-%d") if birth_date is not None else None,
            age,
            record['address'].upper() if record['address'] else None,
            record['city_name'],
            record['city_code'],
            record['district_name'],
            record['district_code'],
            record['village_name'],
            record['village_code'],
            record['is_pns'],
            record['occupation_type'],
            record['occupation_name'].upper(),
            record['workplace_name'].upper(),
            record['symptoms'],
            record['symptoms_notes'],
            record['symptoms_interaction'],
            record['symptoms_activity'],
            record['person_status'],
            register_datetime_local.strftime("%Y-%m-%d %H:%M:%S") if register_datetime_local else None,
            notified_datetime_local.strftime("%Y-%m-%d %H:%M:%S") if notified_datetime_local else None,
            record['attend_location'].upper() if record['attend_location'] else None,
            attended_datetime_local.strftime("%Y-%m-%d %H:%M:%S") if attended_datetime_local else None,
            record['lab_code_sample'],
            result_at_datetime_local.strftime("%Y-%m-%d %H:%M:%S") if result_at_datetime_local else None,
            record['lab_result_type'],
            notified_result_datetime_local.strftime("%Y-%m-%d %H:%M:%S") if notified_result_datetime_local else None,
        ])

    return excel.make_response_from_array(rows, "xlsx", file_name="export", sheet_name="Export")


if __name__ == "__main__":
    # Only for debugging while developing
    app.run(host='0.0.0.0', debug=True)
