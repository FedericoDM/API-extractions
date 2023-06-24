# Class for scrapping DOF with 2023 API

import json
import time
import boto3
import requests

# Local imports
from keys import aws_keys


class DOFScrapper:
    def __init__(self, date):
        # Date
        self.date = date

        # APIs for DOF
        self.FICHAS_API = "https://sidofqa.segob.gob.mx/dof/sidof/notas/fecha"
        self.DIARIO_API = (
            "https://sidofqa.segob.gob.mx/dof/sidof/diarios/porFecha/fecha"
        )
        self.NOTAS_DIARIO_API = "https://sidofqa.segob.gob.mx/dof/sidof/notas/obtenerNotasPorDiario/codDiario"
        self.DOC_NOTA_API = (
            "https://sidofqa.segob.gob.mx/dof/sidof/documentos/doc/codNota"
        )
        self.PDF_DIARIO_API = (
            "https://sidofqa.segob.gob.mx/dof/sidof/documentos/pdf/codDiario"
        )

        # S3
        self.bucket_name = "your-bucket-name"
        self.aws_session = boto3.Session(
            aws_access_key_id=aws_keys["ACCESS_KEY"],
            aws_secret_access_key=aws_keys["SECRET_KEY"],
        )
        self.s3 = self.aws_session.resource("s3")
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.s3_path = f"dof/{self.date}/"

    def check_file_in_s3(self, bucket_name, file_name):
        """
        Checks if a file exists in S3
        """

        try:
            self.s3.head_object(Bucket=bucket_name, Key=file_name)
            print("File Exists!")
            return True
        except Exception as e:
            print("File {file_name} not found")
            return False

    def get_diario_code(self):
        """
        Gets diario for a single date

        Parameters
        ----------
        date : str
            Date in format dd-mm-yyyy
        """
        FIRST_ELEMENT = 0
        new_diario_api = self.DIARIO_API.replace("fecha", self.date)
        response_diario = requests.get(new_diario_api).json()
        self.diario_dict = {}

        if response_diario["response"] == "NOT_FOUND":
            print(f"Diario not found for {self.date}")
        else:
            self.diario_dict["fecha"] = self.date
            if response_diario["Vespertina"]:
                self.diario_dict["codDiario_vespertino"] = response_diario[
                    "Vespertina"
                ][FIRST_ELEMENT]["codDiario"]
            if response_diario["Matutina"]:
                self.diario_dict["codDiario_matutino"] = response_diario["Matutina"][
                    FIRST_ELEMENT
                ]["codDiario"]

    def get_notas_codes(self):
        """
        Getting response from notas API for each diario in diarios_dicts
        """
        if "codDiario_vespertino" in self.diario_dict.keys():
            cod_diario_vesp = self.diario_dict["codDiario_vespertino"]
            success = False
            while not success:
                try:
                    new_notas_diario_api = self.NOTAS_DIARIO_API.replace(
                        "codDiario", str(cod_diario_vesp)
                    )
                    response_notas_diario = requests.get(new_notas_diario_api).json()
                    success = True
                    self.diario_dict["notas_vesp"] = response_notas_diario["Notas"]
                    print(f"Success for {self.diario_dict['fecha']} - notas vespertino")
                except Exception as err:
                    print(err)
                    print(f"Error for {self.diario_dict['fecha']} - notas vespertino")
                    time.sleep(3)
                    continue

        if "codDiario_matutino" in self.diario_dict.keys():
            success = False
            cod_diario_mat = self.diario_dict["codDiario_matutino"]
            while not success:
                try:
                    new_notas_diario_api = self.NOTAS_DIARIO_API.replace(
                        "codDiario", str(cod_diario_mat)
                    )
                    response_notas_diario = requests.get(new_notas_diario_api).json()
                    success = True
                    self.diario_dict["notas_mat"] = response_notas_diario["Notas"]
                    print(f"Success for {self.diario_dict['fecha']} - notas matutino")
                except Exception as err:
                    print(err)
                    print(f"Error for {self.diario_dict['fecha']} - notas matutino")
                    time.sleep(3)
                    continue

    def upload_notas_docs(self):
        """
        Downloads the word document from the requested
        note and uploads it to the S3 bucket
        """
        if "notas_vesp" in self.diario_dict:
            for nota_dict in self.diario_dict["notas_vesp"]:
                nota_code = nota_dict["codNota"]
                key = self.s3_path + f"nota_{nota_code}.doc"
                if not self.check_file_in_s3("dive-cmm", key):
                    nota_api = self.DOC_NOTA_API.replace("codNota", str(nota_code))
                    print(f"Downloading nota {nota_code}")
                    success = False
                    while not success:
                        try:
                            r = requests.get(nota_api, stream=True)
                            success = True
                        except Exception as err:
                            print(err)
                            print(f"Error for {nota_code}")
                            time.sleep(3)
                            continue
                    print(f"Uploading nota {nota_code}")
                    # Upload to S3
                    self.bucket.upload_fileobj(
                        r.raw, key, ExtraArgs={"ACL": "public-read"}
                    )
                    time.sleep(0.5)
                else:
                    print(f"File {key} already exists")

        if "notas_mat" in self.diario_dict:
            for nota_dict in self.diario_dict["notas_mat"]:
                nota_code = nota_dict["codNota"]
                key = self.s3_path + f"nota_{nota_code}.doc"
                if not self.check_file_in_s3("dive-cmm", key):
                    nota_api = self.DOC_NOTA_API.replace("codNota", str(nota_code))
                    print(f"Downloading nota {nota_code}")
                    r = requests.get(nota_api, stream=True)
                    success = False
                    while not success:
                        try:
                            r = requests.get(nota_api, stream=True)
                            success = True
                        except Exception as err:
                            print(err)
                            print(f"Error for {nota_code}")
                            time.sleep(3)
                            continue
                    print(f"Uploading nota {nota_code}")
                    # Upload to S3
                    self.bucket.upload_fileobj(
                        r.raw, key, ExtraArgs={"ACL": "public-read"}
                    )
                    time.sleep(0.5)
                else:
                    print(f"File {key} already exists")

    def upload_diario_pdf(self):
        """
        Uploads the whole diario as pdf
        """
        if "codDiario_vespertino" in self.diario_dict:
            diario_code = self.diario_dict["codDiario_vespertino"]
            pdf_api = self.PDF_DIARIO_API.replace("codDiario", str(diario_code))
            print(f"Downloading diario pdf {diario_code}")
            success = False
            while not success:
                try:
                    r = requests.get(pdf_api, stream=True)
                    success = True
                except Exception as err:
                    print(err)
                    print(f"Error for {diario_code}")
                    time.sleep(3)
                    continue
            print(f"Uploading diario {diario_code}")
            # Upload to S3
            key = self.s3_path + f"diario_{diario_code}.pdf"
            self.bucket.upload_fileobj(r.raw, key)

        if "codDiario_matutino" in self.diario_dict:
            diario_code = self.diario_dict["codDiario_matutino"]
            pdf_api = self.PDF_DIARIO_API.replace("codDiario", str(diario_code))
            print(f"Downloading diario {diario_code}")
            success = False
            while not success:
                try:
                    r = requests.get(pdf_api, stream=True)
                    success = True
                except Exception as err:
                    print(err)
                    print(f"Error for {diario_code}")
                    time.sleep(3)
                    continue
            print(f"Uploading diario {diario_code}")
            # Upload to S3
            key = self.s3_path + f"diario_{diario_code}.pdf"
            self.bucket.upload_fileobj(r.raw, key)

    def upload_diario_json(self):
        """
        Uploads self.diario_dict as json
        """
        if self.diario_dict:
            key = self.s3_path + f"diario_{self.date}.json"
            self.bucket.put_object(Key=key, Body=json.dumps(self.diario_dict))
            print(f"Uploaded diario {self.date} as json")
