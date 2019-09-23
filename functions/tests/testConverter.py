from __future__ import absolute_import

import argparse
from builtins import str
import csv
import datetime
import json
import ntpath
import re
from symbol import except_clause

from apache_beam.coders.coders import Coder
from apache_beam.options import pipeline_options
from numpy import require

import apache_beam as beam


# import google.cloud
class RowTransformer(object):
    '''
    This class is used to transform the rows of the input file in a format
    that can be written in BigQuery
    '''

    def __init__(self, row_delimiter, field_delimiter, header, filename, load_dt, op_dict, conversion=True):
        '''
        We are defining the parameters needed for dataflow pipeline
        '''
        self.field_delimiter = field_delimiter
        self.row_delimiter = row_delimiter
        self.load_dt = load_dt
        self.filename = filename
        self.op_dict = json.loads(op_dict)
        self.conversion = conversion
        # Extract the field name keys from the comma separated input.
        self.keys = re.split(",", header)

    def parse(self, p_row):
        '''
        input: p_row (string)
        output: replace_delimiter (dictionary)
        this function transforms a row file input into a dictionary
        '''
        # delimiter parsing
        row = p_row
        '''
        if self.field_delimiter == '","':  # SLF first delimiter issue resolver
            pos_del = p_row.find(",")
            row = p_row[:pos_del]+' "'+p_row[pos_del:]
        '''
        replace_delimiter = row
        if self.field_delimiter != ";":  # we are going to use a default delimiter
            replace_delimiter = row.replace(";", " ").replace(self.field_delimiter, ";")
        values = re.split(";", re.sub(r'[\r\n"]', '', replace_delimiter))
        while len(values) < len(self.keys):  # It is useful when we have incomplete rows
            values.append(" ")
        replace_delimiter = dict(zip(self.keys, values))
        replace_delimiter['LOAD_TS'] = self.load_dt
        replace_delimiter['SOURCE_ID'] = self.filename
        return replace_delimiter

    def DPLF_run_row_conversion(self, p_row):
        """
        :param p_row: a dictionary representing a row of data, with the field name as the key
        :param p_operations: a dictionary returned by the DPLF_Check_Conversion function
        :return: a list of strings corresponding to the row of data, after some conversion operations were run on them
        """
        p_operations = self.op_dict

        def check_datetime(p_datetime):
            '''
            This function converts a datetime value of the input file
            into a BigQuery datetime format
            input: string
            output: string or None type object
            '''
            import datetime
            try:
                if str(p_datetime).strip() == "" or str(p_datetime).strip().upper() == "NULL" or len(str(p_datetime).strip()) == 1:
                    return None
                else:
                    check = True
                    inner_result = str(p_datetime).strip()
                    if str(p_datetime.strip())[0] == '"':
                        inner_result = inner_result[1:]
                    if str(p_datetime.strip())[-1] == '"':
                        inner_result = inner_result[:-1]
                    if str(inner_result.strip())[10] == "T":
                        inner_result = inner_result.strip()[:10] + " " + inner_result.strip()[11:]
                    if str(inner_result.strip())[-1] == "Z":
                        inner_result = inner_result.strip()[:-1]
                    try:
                        datetime.datetime.strptime(str(inner_result).strip()[:-1], '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError as e:
                        try:
                            datetime.datetime.strptime(str(inner_result).strip()[:-1], '%Y-%m-%d %H:%M:%S')
                        except ValueError as e2:
                            check = False
                            return "Error: " + str(e2)
                    if check:
                        if '.' in str(inner_result):
                            # YYYY-MM-DDTHH:MM:SS.ffffff
                            return str(datetime.datetime.strptime(str(inner_result).strip()[:-1], '%Y-%m-%d %H:%M:%S.%f'))
                        else:
                            # YYYY-MM-DDTHH:MM:SS
                            return str(datetime.datetime.strptime(str(inner_result).strip()[:-1], '%Y-%m-%d %H:%M:%S'))
                    else:
                        return "Error: " + str(e)
            except Exception as e:
                return "Error: " + str(e)

        def date_converter(p_date, p_input_format, p_output_format):
            '''
            This function converts a datetime value of the input file
            into a BigQuery date format
            input: (string, string, string)
            output: string or None type object
            '''
            try:
                if p_date.upper().strip() == "NULL" or len(p_date.strip()) < 1 or p_date.strip() == "":
                    return None
                else:
                    input_order = sorted(['Y', 'M', 'D'],
                                         key=lambda x: p_input_format.index(x))
                    output_order = sorted(['Y', 'M', 'D'],
                                          key=lambda x: p_output_format.index(x))
                    input_digits = {x: p_input_format.count(x) for x in ['Y', 'M', 'D']}
                    output_digits = {x: p_output_format.count(x) for x in ['Y', 'M', 'D']}
                    input_delimiter = p_input_format[input_digits[input_order[0]]:p_input_format.index(input_order[1])]
                    output_delimiter = p_output_format[output_digits[output_order[0]]:p_output_format.index(output_order[1])]
                    input_values = p_date.split(input_delimiter)
                    output_values = [input_values[input_order.index(value)] for value in output_order]
                    inner_result = output_delimiter.join(output_values)
                    # date.fromisoformat(inner_result)
                    return inner_result
            except Exception as e:
                return "Error: " + str(e)

        def numeric_converter(p_num, p_d_sep, p_t_sep):
            '''
            This function converts a numeric value of the input file
            into a BigQuery numeric format
            input: (string, string, string)
            output: string numerical object
            '''
            try:
                if p_num.upper().strip() == "NULL" or len(p_num.strip()) < 1 or p_num.strip() == "" or p_num.strip() == "\n" or p_num.strip == "\t":
                    inner_result = "0"
                else:
                    if p_t_sep == "No Delimiter":
                        if p_d_sep != ".":
                            s = str(p_num)
                            s2 = s.replace(p_d_sep, ".")
                            inner_result = s2.strip()
                            if len(inner_result) > 10:
                                spl = inner_result.split(".")
                                floor = spl[0]
                                decimal = spl[1]
                                if len(decimal) > 9:
                                    inner_result = str(floor)+"."+str(decimal[:9])
                                return inner_result
                        else:
                            inner_result = str(p_num).strip()
                            if len(inner_result) > 10:
                                spl = inner_result.split(".")
                                floor = spl[0]
                                decimal = spl[1]
                                if len(decimal) > 9:
                                    inner_result = str(floor)+"."+str(decimal[:9])
                                return inner_result
                    else:
                        input_s = str(p_num)
                        str2 = input_s
                        if input_s != '':
                            l_s = input_s.split(p_t_sep)
                            str2 = ''
                            for s in l_s:
                                str2 = str2 + s
                            if p_d_sep != ".":
                                str2 = str2.replace(p_d_sep, ".")
                        inner_result = str2.strip()
                    if len(inner_result) > 10:
                        spl = inner_result.split(".")
                        floor = spl[0]
                        decimal = spl[1]
                        if len(decimal) > 9:
                            inner_result = str(floor)+"."+str(decimal[:9])
                float(inner_result)
                return inner_result
            except Exception as e:
                return "Error: " + str(e)

        def string_check(p_input):
            '''
            This function checks if a string of the input file
            is a null string and returns a None object in this case
            input: string
            output: string or None type object
            '''
            p_string = p_input.strip()
            if len(p_string) < 1 or p_string.upper() == "NULL":
                inner_result = None
            else:
                inner_result = p_string
            return inner_result
        #  Now we are obtaining the dictionary that contains the couples key-value that will be written into BigQuery tables
        if p_row is not None:
            op_dictionary = {'DATE': date_converter,
                             'NUMERIC': numeric_converter,
                             'DATETIME': check_datetime,
                             'None': string_check}

            zip_dict = {}
            for field in p_operations:
                try:
                    key = str(field).strip()
                    value1=p_row[str(field).strip()]
                    value2=p_operations[str(field).strip()]
                    valueTot= (value1, value2)
                    zip_dict[key] = valueTot
                except Exception as e:
                    return("Error: " + str(e))
            
            
            result = {str(field).strip(): op_dictionary[op[0]](value, *op[1:]) for field, (value, op) in zip_dict.items()}
            check = True
            result['LOAD_TS'] = self.load_dt
            result['SOURCE_ID'] = self.filename
            for field in result.keys():
                if type(result[field]) == str:
                    if result[field].find("Error") >= 0:
                        check = False
            return result, check


class ISOCoder(Coder):
    # Set up a coder used for reading and writing strings as ISO-8859-1
    def encode(self, value):
        return value.encode("ISO-8859-1")

    def decode(self, value):
        return value.decode("ISO-8859-1")

    def is_deterministic(self):
        return True


inputFile = "csv/20190923100000_01_PLACE__C.csv"
fields= "CREATEDBYID,CREATEDDATE,ID,ISDELETED,LASTMODIFIEDBYID,LASTMODIFIEDDATE,LASTREFERENCEDDATE,LASTVIEWEDDATE,NAME,OWNERID,SYSTEMMODSTAMP,ACCOUNT__C,ACCOUNT_CHANGED__C,ACCOUNT_OLD__C,APPROVAL_STATUS__C,CITY__C,COMPANY__C,COUNTRY__C,DATASOURCE__C,EMAIL__C,FIRSTNAME__C,GOLDENRECORDID__C,INITIAL_SUBMITTER__C,LASTNAME__C,LEAD__C,LEAD_CHANGED__C,LEAD_OLD__C,MOBILEPHONE__C,NEW_ACCOUNT__C,NEW_LEAD__C,NX_AMOUNT__C,NX_PAID__C,NX_REFUNDSTATUS__C,OLDREFERENCE__C,PHONE__C,PLACE_TIMESTAMP__C,PLACE_TYPE__C,PLACEID__C,PLACEIDALIASES__C,PLACESTATUS__C,PO_BOX__C,PO_BOX_CITY__C,PO_BOX_ZIP_POSTAL_CODE__C,POSTALCODE__C,PROCESS__C,PRODUCT_INFORMATION__C,QUOTE_MIGRATION_BATCH__C,QUOTEITEMID__C,SAMBACONTRACT__C,SEARCHFIELD_ADDRESS__C,SEARCHFIELD_PHONEEMAIL__C,SL_MIGRATION__C,SLMIG_AMOUNTCATEGORY__C,SLMIG_AMOUNTLOCATION__C,SLMIG_RELATED_LCMCOMPLEX__C,SLMIG_STARTDATE__C,STATUS__C,STREET__C,SUBSOURCE__C,TITLE__C,VIEW_ON_AMA__C,VIEW_ON_CC__C,VIEW_ON_LOCAL_CH__C,VIEW_ON_SEARCH_CH__C,WEBSITE__C"
field_delimiter="," 
row_delimiter="\n"
load_dt="2019-09-10T10:48:02"
conversion=True
op_dict="{\"CREATEDBYID\": [\"None\"], \"CREATEDDATE\": [\"None\"], \"ID\": [\"None\"], \"ISDELETED\": [\"None\"], \"LASTMODIFIEDBYID\": [\"None\"], \"LASTMODIFIEDDATE\": [\"None\"], \"LASTREFERENCEDDATE\": [\"None\"], \"LASTVIEWEDDATE\": [\"None\"], \"NAME\": [\"None\"], \"OWNERID\": [\"None\"], \"SYSTEMMODSTAMP\": [\"None\"], \"ACCOUNT__C\": [\"None\"], \"ACCOUNT_CHANGED__C\": [\"None\"], \"ACCOUNT_OLD__C\": [\"None\"], \"APPROVAL_STATUS__C\": [\"None\"], \"CITY__C\": [\"None\"], \"COMPANY__C\": [\"None\"], \"COUNTRY__C\": [\"None\"], \"DATASOURCE__C\": [\"None\"], \"EMAIL__C\": [\"None\"], \"FIRSTNAME__C\": [\"None\"], \"GOLDENRECORDID__C\": [\"None\"], \"INITIAL_SUBMITTER__C\": [\"None\"], \"LASTNAME__C\": [\"None\"], \"LEAD__C\": [\"None\"], \"LEAD_CHANGED__C\": [\"None\"], \"LEAD_OLD__C\": [\"None\"], \"MOBILEPHONE__C\": [\"None\"], \"NEW_ACCOUNT__C\": [\"None\"], \"NEW_LEAD__C\": [\"None\"], \"NX_AMOUNT__C\": [\"None\"], \"NX_PAID__C\": [\"None\"], \"NX_REFUNDSTATUS__C\": [\"None\"], \"OLDREFERENCE__C\": [\"None\"], \"PHONE__C\": [\"None\"], \"PLACE_TIMESTAMP__C\": [\"None\"], \"PLACE_TYPE__C\": [\"None\"], \"PLACEID__C\": [\"None\"], \"PLACEIDALIASES__C\": [\"None\"], \"PLACESTATUS__C\": [\"None\"], \"PO_BOX__C\": [\"None\"], \"PO_BOX_CITY__C\": [\"None\"], \"PO_BOX_ZIP_POSTAL_CODE__C\": [\"None\"], \"POSTALCODE__C\": [\"None\"], \"PROCESS__C\": [\"None\"], \"PRODUCT_INFORMATION__C\": [\"None\"], \"QUOTE_MIGRATION_BATCH__C\": [\"None\"], \"QUOTEITEMID__C\": [\"None\"], \"SAMBACONTRACT__C\": [\"None\"], \"SEARCHFIELD_ADDRESS__C\": [\"None\"], \"SEARCHFIELD_PHONEEMAIL__C\": [\"None\"], \"SL_MIGRATION__C\": [\"None\"], \"SLMIG_AMOUNTCATEGORY__C\": [\"None\"], \"SLMIG_AMOUNTLOCATION__C\": [\"None\"], \"SLMIG_RELATED_LCMCOMPLEX__C\": [\"None\"], \"SLMIG_STARTDATE__C\": [\"None\"], \"STATUS__C\": [\"None\"], \"STREET__C\": [\"None\"], \"SUBSOURCE__C\": [\"None\"], \"TITLE__C\": [\"None\"], \"VIEW_ON_AMA__C\": [\"None\"], \"VIEW_ON_CC__C\": [\"None\"], \"VIEW_ON_LOCAL_CH__C\": [\"None\"], \"VIEW_ON_SEARCH_CH__C\": [\"None\"], \"WEBSITE__C\": [\"None\"]}"



def run(**argv):
    
#     # Main function which defines and runs the pipeline that are then executed by Dataflow 
#     parser = argparse.ArgumentParser()
#     # Add the arguments needed for this Dataflow job
#     parser.add_argument(
#         'input',metavar='input', 
#         help='Input file to read')
# 
#     parser.add_argument('output', metavar='output',
#                         help='Output BQ table to write results to',)
# 
#     parser.add_argument('delimiter',  
#                         help='Delimiter to split input records',
#                         default=';')
# 
#     parser.add_argument('fields')
# 
#     parser.add_argument('load_dt')
# 
#     parser.add_argument('op_dict')
# 
#     
#     known_args, pipeline_args = parser.parse_known_args(argv)

    row_transformer = RowTransformer(row_delimiter=row_delimiter,
                                     field_delimiter=field_delimiter,
                                     header=fields,
                                     filename=ntpath.basename(inputFile),
                                     load_dt=load_dt,
                                     op_dict=op_dict,
                                     conversion=conversion)

#     p_opts = pipeline_options.PipelineOptions(pipeline_args)

    # Initiate the pipeline 
#     with beam.Pipeline(options=p_opts) as pipeline:
    # Read the file
#     rows =  beam.io.ReadFromText(inputFile,skip_header_lines=1,coder=ISOCoder())
    
    with open(inputFile) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        count = 0
        for row in readCSV:
            if(count>0):
                rowstr = json.dumps(row)
                r = row_transformer.parse(rowstr)
                row_transformer.DPLF_run_row_conversion(r)
            count=count+1

#     # Stage of the pipeline that translates a delimited single row to a dictionary object consumable by BigQuery.
#     dict_records = rows | "Convert to BigQuery row" >> beam.Map(
#         lambda r: row_transformer.parse(r))


#     conv_check = dict_records | "Datatype Check and Conversion" >> beam.Map(
#         lambda d: row_transformer.DPLF_run_row_conversion(d)
#     )


run()
