#!/usr/bin/python
# -*- coding: utf-8 -*-

import httplib
import urllib as ul
import boto
from boto.glacier.layer1 import Layer1
from boto.glacier.vault import Vault
from boto.glacier.job import Job
from boto.glacier.concurrent import ConcurrentUploader

import sys
import os.path
import json
import datetime
from optparse import OptionParser

# modulo para serializar el diccionario con la data

try:
    import cPickle as pickle
except ImportError:
    import pickle


class Glacier:

    def __init__(self, jobid=None):
        self.access_key_id = 'YOUR OWN access_key_id'
        self.secret_key = 'YOUR OWN secret key'
        self.target_vault_name = 'YOUR VAULT NAME'
        self.region = 'us-west-2'
        if jobid:
            self.jobid = jobid
        else:
            self.jobid = None

    def request(self, tipo=None):
        if tipo == 'inventory':
            params = {'Description': 'inventory-job',
                      'Type': 'inventory-retrieval', 'Format': 'CSV'}
        elif tipo == 'file':
            params = {'ArchiveId': self.archid,
                      'Description': 'retrieval-job',
                      'Type': 'archive-retrieval'}
        else:
            print 'necesita un tipo de operacion'
            sys.exit(1)

        glacier_layer1 = Layer1(aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_key,
                                region_name=self.region)
        print 'operation starting...'
        job_id = glacier_layer1.initiate_job(self.target_vault_name,
                params)
        print 'inventory job id: %s' % (job_id, )
        print 'Operation complete.'
        return job_id

    def get_inventory(self):
        glacier_layer1 = Layer1(aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_key,
                                region_name=self.region)
        print 'query operation starting...'
        if self.jobid != None:
            f = glacier_layer1.get_job_output(self.target_vault_name,
                    self.jobid)
        else:
            f = glacier_layer1.list_jobs(self.target_vault_name)
            if f['Marker']:
                salida = True
                mark = f['Marker']
                while salida:
                    g = \
                        glacier_layer1.list_jobs(self.target_vault_name,
                            marker=mark)
                    if g['Marker'] == None:
                        salida = False
                    else:
                        mark = g['Marker']
                        f['JobList'] = f['JobList'] + g['JobList']
        print 'query operation complete.'
        return f

    def just_write(self, filename, content):
        file = open(filename, 'a')
        file.write(content)
        file.close()

    def get_file(self, size, filename):
        glacier_layer1 = Layer1(aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_key,
                                region_name=self.region)
        print 'download operation starting...'
        if size:
            maxrange = size
            chunks = 10
            chsize = size / chunks
            rn = 0
            cnt = 1
            max = chsize
            min = 0
            tot_downloaded = 0
        if self.jobid != None:

            for i in range(1, chunks + 1):
                print 'download chunk %s / %s' % (cnt, chunks)
                rango = (min, max)
                print rango
                f = \
                    glacier_layer1.get_job_output(self.target_vault_name,
                        self.jobid, byte_range=rango)
                rn += 1
                cnt += 1
                min = chsize * rn + rn
                max = chsize * cnt + rn

                try:  # truco por bug con httplib
                    contents = f.read()
                except httplib.IncompleteRead, e:
                    contents = e.partial

                self.just_write(filename, contents)

                tot_downloaded += chsize

            if tot_downloaded < size and size - tot_downloaded > 1:  # por redondeo puede quedar pequenas diferencia
                rango = (tot_downloaded + 1, size)  # nuevo rango desde los descarga hasta el tamano de archivo
                diff = size - tot_downloaded
                print 'Descargando diferencia por factor de redondeo de %s Bytes' \
                    % diff
                f = \
                    glacier_layer1.get_job_output(self.target_vault_name,
                        self.jobid, byte_range=rango)
                try:
                    contents = f.read()
                except httplib.IncompleteRead, e:
                    contents = e.partial

                self.just_write(filename, contents)  # write final del pedacito quede sin descargar
        else:
            f = glacier_layer1.list_jobs(self.target_vault_name)
        print 'Download Operation complete.'

    def queue_file(self, aws_id, filedata=None):
        datafile = 'data.dat'
        if os.path.exists(datafile):
            fichero = file(datafile)
            data = pickle.load(fichero)
            fichero.close()
        else:
            data = {}
        if aws_id in data.keys():
            print 'AWS id ya esta esperando para ser descargado use la opcion -d para verificar si esta ya disponible'
        else:
            self.archid = aws_id  # el id de glacier a descargar
            f = self.request('file')  # ahora hago el request en si para el id de glacier
            data[aws_id] = {'filedata': filedata, 'job_id': f['JobId']}  # dict con mi data

            # print f

            if f:
                print 'AWS id agregado a la cola de descarga exitosamente'
            else:
                print 'error intentado agregar'

        fichero = file(datafile, 'w')
        pickle.dump(data, fichero, 2)
        fichero.close()

    def get_filedata(self, archid):
        datafile = 'data.dat'

        # print jobid

        if os.path.exists(datafile):
            fichero = open(datafile)
            data = pickle.load(fichero)
            fichero.close()
        else:
            raise Exception('No existe archivo de datos')

        if archid in data.keys():
            retorno = data[archid]['filedata']
        else:
            retorno = None

        return retorno

    def delete_job(self, archid):
        datafile = 'data.dat'
        if os.path.exists(datafile):
            fichero = file(datafile)
            data = pickle.load(fichero)
            fichero.close()
        else:
            raise Exception('No hay data disponible')
        if archid in data.keys():
            print 'borrando job en data local'
            del data[archid]
            fichero = file(datafile, 'w')
            pickle.dump(data, fichero, 2)
            fichero.close()
        else:
            print 'Nada que borrar'

    def upload_to_aws(self, fname, desc):
        glacier_layer1 = Layer1(aws_access_key_id=self.access_key_id,
                                aws_secret_access_key=self.secret_key,
                                region_name=self.region)
        uploader = ConcurrentUploader(glacier_layer1,
                self.target_vault_name, 32 * 1024 * 1024)
        if os.path.exists(fname):
            print 'subiendo  a glacier...'
            archive_id = uploader.upload(fname, desc)
            if archive_id:
                print 'Archivos subido a glacier on exito con id:%s' \
                    % archive_id
                return archive_id
        else:
            print 'No se encontro el archivo: %s' % fname


if __name__ == '__main__':
    usage = 'utilizacion: %prog [options] '
    parser = OptionParser(usage)
    parser.add_option('-i', '--inventory', action='store_true',
                      dest='inventory',
                      help='Solicita un inventario de Glacierv \n Puede haber solo uno en cola. Opcion -d para descargar cualquier job listo'
                      )
    parser.add_option('-a', '--aws', action='store', dest='aws',
                      help='Recibe un Id Glacier a descargar a un archivo \n  Requiere un nombre de archivo. Opcion -d para descargar cualquier job listo'
                      )
    parser.add_option('-f', '--filename', action='store',
                      dest='filename',
                      help='Nombre del archivo a subir o a bajar(incluir extension)'
                      )
    parser.add_option('-d', '--descarga', action='store_true',
                      dest='descarga',
                      help='Efectivamente descarga jobs de archivos completados'
                      )
    parser.add_option('-c', '--consulta', action='store_true',
                      dest='consulta',
                      help='Muestra en pantalla los jobs pendientes en glacier y la metadatalocal'
                      )
    parser.add_option('-u', '--upload', action='store_true',
                      dest='upload', help='Sube un archivo a glacier')
    parser.add_option('-s', '--desc', action='store', dest='desc',
                      help='Descripcion del archivo a subir a glacier')
    parser.add_option('-k', '--keywords', action='store', dest='keys',
                      help='Tags separados por coma')

    (options, args) = parser.parse_args()

    gl = Glacier()  # Instancio Glacier

    if options.inventory:  # si solicita un job....de descarga de inventario

        f = gl.get_inventory()  # primero chequeo que no hayan previos corriendo (inventarios)
        data = json.load(f)

        if len(data['JobList']) > 0:  # si hay data de jobs

            for jb in data['JobList']:  # verifico que no haya ya un inventario solicitado
                if jb['JobDescription'] == u'inventory-job' \
                    and jb['Completed'] == False:
                    print 'Ya hay un inventario pendiente por descargar'
                    print 'con el JobId: %s' % jb['JobId']
        else:

            print 'Solicitando inventario'
            gl.request('inventory')  # si no hay solicitado un inv previo, solicitar
    elif options.aws and options.filename and options.keywords:

                                     # si solicita un job de descarga de archivo
        filedata={}
        filedata["filename"]=options.filename
        filedata["tags"]=options.keywords
        filedata["descripcion"]=options.desc or ""
        f = gl.queue_file(options.aws, filedata)
    
    elif options.descarga:

        f = gl.get_inventory()  # verifico que es lo que esta corriendo

        # data=json.load(f)........#para verificar que esta listo para descarga y que no
        # print len(data['JobList'])
        # print len(f['JobList'])

        if len(f['JobList']) > 0:  # si hay data de jobs

            for jb in f['JobList']:  
                fecha = \
                    datetime.datetime.now().strftime('%d-%m-%Y_%H:%M:%S'
                        )

                if jb['JobDescription'] == u'inventory-job' \
                    and jb['Completed'] == True:
                    gl.jobid = jb['JobId']
                    g = gl.get_inventory()
                    file = open('inventory_' + fecha + '.csv', 'w')
                    file.write(ul.unquote_plus(g.read()))
                    file.close()
                    gl.jobid = None

                if jb['JobDescription'] == u'retrieval-job' \
                    and jb['Completed'] == True :
                    #and "2014-10-29" in jb['CompletionDate']
                    #print jb['CompletionDate'],type(jb['CompletionDate'])
                     # print "entra"

                    gl.jobid = jb['JobId']
                    filedata = gl.get_filedata(jb['ArchiveId'])
                    if filedata:
                        filename = filedata["filename"]
                        # print jb['JobId']
                        # print "filename es:",filename[1]

                        if filename:
                            print 'descargando...'
                            size = jb['ArchiveSizeInBytes']

                        # print size

                            gl.get_file(size, filename)
                            gl.delete_job(jb['ArchiveId'])
                    else:
                        print 'No se encontro data local para el job: %s' \
                            % jb['JobId']
        else:

                    # gl.jobid=None

            print 'No hay jobs listos para descargar'
    elif options.consulta:

        f = gl.get_inventory()  # verifico que es lo que esta corriendo

        # data=json.load(f)........#para verificar que esta listo para descarga y que no

        print json.dumps(f, indent=2)
        print 'total data remota: %s' % len(f['JobList'])
        print '###################################################################'
        print '######################### META DATA LOCAL #########################'
        print '###################################################################'
        datafile = 'data.dat'

        if os.path.exists(datafile):
            fichero = file(datafile, 'r')
            data = pickle.load(fichero)
            fichero.close()
        print json.dumps(data, indent=2)
        print 'total data local: %s' % len(data)
    elif options.upload:

        if options.desc == None:
            print 'Necesita una descripcion de archivo'
        elif options.filename == None:
            print 'Necesita un nombre de archivo en disco para subir'
        else:
            gl.upload_to_aws(options.filename, options.desc)



# {
#       "CompletionDate": "2014-10-29T20:12:25.397Z", 
#       "InventoryRetrievalParameters": null, 
#       "RetrievalByteRange": "0-96576217", 
#       "SHA256TreeHash": "21741b478d589212e83188b42f2487edf655b79700a108ad7e297b22d0b2b286", 
#       "Completed": true, 
#       "InventorySizeInBytes": null, 
#       "VaultARN": "arn:aws:glacier:us-west-2:750589158960:vaults/EUTV-HQ-2", 
#       "JobId": "J16b-NpfinlmTeskRC0jkAKpwyLoOrZdNhtkJHW0WnHxZn11Rjsv-b_8jQb6UkbcDBjefyKpDhdgkwGYChXrCVZL-ZrI", 
#       "ArchiveId": "Ey2RQwx3kUpRHpa6f_KSs2XOdRq6QT7XQl4NPB5U7s5QRmhraawq4NQSbFe0s8UX_CAONYAOXKTRn7D_mBKKgn99yTCOLCHyx3LDJTK-yKzah3-FhxU_KcO_XFOtkvm-OpGeO_f7Wg", 
#       "JobDescription": "retrieval-job", 
#       "StatusMessage": "Succeeded", 
#       "StatusCode": "Succeeded", 
#       "Action": "ArchiveRetrieval", 
#       "ArchiveSHA256TreeHash": "21741b478d589212e83188b42f2487edf655b79700a108ad7e297b22d0b2b286", 
#       "CreationDate": "2014-10-29T16:20:16.722Z", 
#       "SNSTopic": null, 
#       "ArchiveSizeInBytes": 96576218
#     }

