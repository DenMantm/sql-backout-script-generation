'''
Running from Git Bash
paste this line into ~/.bashrc file:
    alias python='winpty python.exe'

Usage:
    help:
        python sysimp_verify.py -h
    verfify & backout:
        python sysimp_verify.py -dml <path to dml script>

    prompts:
        - db username: e.g. simp_<windows id>
        - db SID: e.g. bw3_qa
        - Password: <database password>

Description
- insert, update and delete statement strings are
  found and applied to the inputted db using cx_Oracle.
  rollback is executed at the end.
  columns & values affected by these statements are collected
  in ConfigDict class's init method
- verify the dml script (deploy to the db with sqlplus)
  if no errors in the cx_Oracle step.
- if verify step ok, create the backout script using the data
  created by ConfigDict
- verify the backout script by running dml, backout, dml.
'''

import argparse
import cx_Oracle
import datetime
import getpass
import os
import re
import subprocess
import sys
import time

this_dir = os.path.dirname(__file__)


class ConfigDict:

    '''
    Return a dictionary of form:
        { table-name: [ (column-name, value) ] }
    for each insert and update in configuation.sql
    '''

    def __init__(self, conn_str, dmlpath, cx_Oracle_logfile):
        self.db_conn = cx_Oracle.Connection(conn_str)
        self.dmlpath = dmlpath
        self.cx_Oracle_logfile = os.path.join(this_dir, cx_Oracle_logfile)

        cursor = self.db_conn.cursor()
        self.cursor = cursor
        self.actual_tables = []
        self.column_dict = {}
        self.line_list = []
        self.current_line_num = None
        self.updates = []
        self.update_line_nums = []
        self.update_tables = []
        self.pre_update = []
        self.post_update = []
        self.inserts = []
        self.insert_statements = []
        self.insert_line_nums = []
        self.insert_tables = []
        self.deletes = []
        self.delete_statements = []
        self.delete_line_nums = []
        self.delete_tables = []
        self.del_or_up = []
        self.insert = '''(insert\s+into\s+
                         (?P<INSERT_TABLE>[^(]*)
                         .*?;)\s*([\n]|$)'''
        self.update = '''(update\s+
                        (?P<UPDATE_TABLE>[^\s]*)
                        .*?where
                        .*?;)\s*([\n]|$)'''
        self.delete = '''(delete\s+
                         (?P<DELETE_1>[*]\s+)?
                         (?P<DELETE_2>from\s+)
                         (?P<DELETE_TABLE>[^\s]*)
                         .*?;)\s*([\n]|$)'''

    def config_file_to_string(self, infile=None):
        '''
        turn configuration.sql into list of sql statements
        '''
        if infile is None:
            c = self.dmlpath
        else:
            c = infile
        s = ''
        with open(c, 'r') as f:
            statement = re.compile('(insert|update|delete)',
                                   re.I | re.S)
            line_number = 0
            for line_number, line in enumerate(f, start=1):
                # Does this line contain start of statement
                if statement.match(line.lstrip()):
                    self.line_list.append(line_number)
                # Handle comments
                if '--' in line:
                    pos = line.index('--')
                    if pos == 0:
                        continue
                    elif line[:pos].strip() == '':
                        continue
                    else:
                        s += line[:pos]
                else:
                    s += line
        return s

    def validate_config(self, keyword_list):
        """
        Ensure no 'commit' or 'trigger' filename
        """
        with open(self.dmlpath, 'r') as f:
            f = f.read()
            for word in keyword_list:
                if re.search(word, f, re.I):
                    c = os.path.basename(self.dmlpath)
                    print "'{}' found in {}, exiting".format(word, c)
                    s = ("{} {} {}".format(
                        c,
                        "contains one of the keywords:",
                        keyword_list))
                    with open(self.cx_Oracle_logfile, 'a') as l:
                        l.write('{}'.format(s))
                    exit()

    def process_config(self, infile=None):

        '''
        Return a dictionary.
        key = table name

        value = tuple. index 0: list of sublists. Each sublist is the
                                (column-name,value) of each affected row
                                (insert, delete, update)

                       index 1: line number of statement in configuration.sql
        '''

        s = self.config_file_to_string(infile)

        STATEMENT = '({})|({})|({})'.format(
            self.insert,
            self.update,
            self.delete)

        match_statement = re.compile(STATEMENT, re.I | re.S | re.X)
        pattern_insert = re.compile(self.insert, re.I | re.S | re.X)
        pattern_delete = re.compile(self.delete, re.I | re.S | re.X)
        pattern_update = re.compile(self.update, re.I | re.S | re.X)

        results = {}
        line_list = list(self.line_list)
        statement_matches = match_statement.finditer(s)
        console = ConsoleOut()

        for count, m in enumerate(statement_matches, start=1):
            self.current_line_num = line_list[0]
            d = m.groupdict()
            match_str = m.group()
            
            if sys.platform == 'linux2':
                start_time = time.time()
            else:
                start_time = time.clock()

            match_insert = pattern_insert.match(match_str)
            match_update = pattern_update.match(match_str)
            match_delete = pattern_delete.match(match_str)

            if match_insert:
                d = match_insert.groupdict()
                tn = d['INSERT_TABLE'].strip().lower()
                g = match_insert.group(1)
                g = g.replace('\n', ' ')
                processed_statement = self.process_insert(g, tn)

            elif match_update:
                d = match_update.groupdict()
                g = match_update.group(1)
                g = g.replace('\n', ' ')
                tn = d['UPDATE_TABLE'].strip().lower()
                processed_statement = self.process_update(g, tn)

            elif match_delete:
                d = match_delete.groupdict()
                g = match_delete.group(1)
                g = g.replace('\n', ' ')
                tn = d['DELETE_TABLE'].strip().lower()
                processed_statement = self.process_delete(g, tn)
            else:
                print 'unexpected statement: {}'.format(match_str)
                exit()

            if tn not in results:
                results[tn] = []
                self.actual_tables.append(tn)

            results[tn].append((processed_statement, line_list.pop(0)))

            console.write(tn, self.current_line_num,
                          count, len(line_list), start_time)

        print '\nExecuting rollback'
        sys.stdout.flush()
        self.cursor.execute("rollback")
        print '\nrollback complete'
        sys.stdout.flush()

        self.db_conn.close()

        print '\nstatement list created'
        sys.stdout.flush()

        return results

    def process_insert(self, statement, tn):
        '''
        return list of columns and values of statement
        update self.insert_statements list with statement
        '''
        statement = statement.rstrip(';')
        self.insert_statements.append(statement)
        var = self.cursor.var(cx_Oracle.ROWID)
        statement += ' returning rowid INTO :v '
        try:
            query = statement
            self.cursor.execute(statement, v=var)
            query = "select * from {table_n} where rowid =: v".format(
                table_n=tn)
            self.cursor.execute(query, v=var)
            values = self.cursor.fetchall()[0]
            column_names = tuple(i[0].lower()
                                 for i in self.cursor.description)
            self.column_dict[tn] = column_names
            z = zip(column_names, values)
            self.inserts.append(z)
            self.insert_line_nums.append(self.current_line_num)
            self.insert_tables.append(tn)

            return z

        except cx_Oracle.DatabaseError as e:
            with open(self.cx_Oracle_logfile, 'a') as f:
                f.write('''Database exception: {} Line number:
                          {}\nSQL: {}\n\n'''.format(
                              str(e).strip(), str(
                                  self.current_line_num), query))
        except:
            print 'unknown exception'
            with open(self.cx_Oracle_logfile, 'a') as f:
                f.write('Unknown exception. Line number: {}\n'.format(
                    str(self.current_line_num)))
            raise

    def process_delete(self, delete_statement, tn):
        '''
        return data which is deleted by delete_statement
        append statement to the self.deletes list
        '''
        self.delete_statements.append(delete_statement)
        l = delete_statement.split()

        if re.match('[*]', l[1], re.I):
            l.pop(1)
        if re.match(l[1], 'from', re.I):
            l.pop(1)
        l.pop(0)
        l.insert(0, "SELECT * FROM")
        select_statement = " ".join(l)

        try:
            select_statement = select_statement.rstrip(';')
            self.cursor.execute(select_statement)
            values = self.cursor.fetchall()
            column_names = tuple([i[0].lower()
                                  for i in self.cursor.description])
            self.column_dict[tn] = column_names

            z = [zip(column_names, v) for v in values]
            self.deletes.append(z)
            self.delete_line_nums.append(self.current_line_num)
            self.delete_tables.append(tn)
            '''Execute delete statement'''
            delete_statement = delete_statement.rstrip(';')
            self.cursor.execute(delete_statement)
            print z
            return z

        except cx_Oracle.DatabaseError as e:
            with open(self.cx_Oracle_logfile, 'a') as f:
                f.write(
                    'Database exception: {} Line number: {}\n'.format(
                        str(e).strip(), str(self.current_line_num)))
        except:
            print 'unknown exception'
            with open(self.cx_Oracle_logfile, 'a') as f:
                f.write(
                    'Unknown exception. Line number: {}\n'.format(
                        str(self.current_line_num)))
            raise

    def process_update(self, update, tn):
        '''
        return list of data affected by update
        append update to self.updates list
        '''

        update = update.rstrip(';')

        ''' indexes of where and set in update statement:'''
        where = re.search('where(\s+)', update, re.I)
        set_ = re.search('set(\n|\s+)', update, re.I)
        if where:
            update_where_index = int(update.index(where.group()))
        else:
            print "\n\n'where' statement not found in {}\n".format(update)
            self.cursor.execute("rollback")
            print "\nexiting"
            exit()
        if set_:
            update_set_index = int(update.index(set_.group()))
        else:
            print "\n\n'set' statement not found in: {}\n".format(update)
            print "\nexiting"
            self.cursor.execute("rollback")
            exit()

        '''
        select_pre. Select statement to get the rows that would be updated
        '''
        select_pre = "select * from {} {}".format(
            tn, update[update_where_index:])

        ''' collect update statements '''

        self.updates.append(update)

        try:
            '''
            Execute select_pre and return a
            list of rows as a col val dictionary
            '''
            self.cursor.execute(select_pre)
            result = self.cursor.fetchall()
            cols = [i[0].lower() for i in self.cursor.description]
            vals = [list(v) for v in result]
            pre_update_values = [zip(cols, v) for v in vals]
            self.pre_update.append([[list(z) for z in l]
                                    for l in pre_update_values])

            ''' set_values. The updated columns and their values. '''
            update_values_list = update[
                update_set_index + 4:update_where_index]

            value_pattern = re.compile("\'(.*?)\'")
            value_list = value_pattern.findall(update_values_list)

            column_pattern = re.compile("(\w+)\s*=")
            column_list = column_pattern.findall(update_values_list)

            set_values = dict(zip(column_list, value_list))

            '''
            update self.pre_update_dict with set_values
            to form post_update selects
            '''
            post_up_vals = []
            for pre_d in pre_update_values:
                post_d = list(pre_d)
                for col, val in set_values.items():
                    for ind, value in enumerate(post_d):
                        post_d[ind] = list(value)
                        if col.lower() in value:
                            val = [col, val]
                            post_d[ind] = val
                post_up_vals.append(post_d)
            self.post_update.append(post_up_vals)
            self.update_line_nums.append(self.current_line_num)
            self.update_tables.append(tn)
            return post_up_vals
        except cx_Oracle.DatabaseError as e:
            with open(self.cx_Oracle_logfile, 'a') as f:
                f.write(
                    'Database exception: {} Line number: {}\n'.format(
                        str(e).strip(), str(self.current_line_num)))
        except:
            print 'unknown exception'
            with open(self.cx_Oracle_logfile, 'a') as f:
                f.write('Unknown exception. Line number: {}\n'.format(
                    str(self.current_line_num)))
            raise


class ConsoleOut:
    def __init__(self):

        self.o = sys.stdout
        self.current_table = 'None'
        self.statement_count = 0
        self.time_taken = 0

        self.template = "{:<40} | {:<17} | {:<14} | {:<16} | {:<11} | {:<11}"

        self.o.write(
            self.template.format('Table',
                                 'Statement count',
                                 'Current line',
                                 'Statement total',
                                 'Remaining',
                                 'Time taken'))

    def write(self, table, current_line,
              total, remaining, start_time):
        if sys.platform == 'linux2':
            time_delta = time.time() - start_time
        else:
            time_delta = time.clock() - start_time
        self.time_taken += time_delta

        def out(statement_count):
            return self.template.format(
                table,
                statement_count,
                current_line,
                total,
                remaining,
                self.time_taken)

        if table == self.current_table:
            self.statement_count += 1

            if sys.platform == 'linux2':
                pass
            else:
                self.o.write('\r' + out(self.statement_count))

        else:
            self.time_taken = 0
            self.statement_count = 1
            self.o.write('\n' + out(self.statement_count))

        self.current_table = table
        self.o.flush()


class Db:
    CONFIG_OUT = os.path.join(this_dir, 'out.sql')

    def __init__(self, dmlpath, backout_path, results, sqlplus_logfile,
                 db_connection_string,validation_path):

        self.sqlplus_verify_logfile = os.path.join(this_dir, sqlplus_logfile
                                                   + '_verify.log')
        self.sqlplus_backout_logfile = os.path.join(this_dir, sqlplus_logfile
                                                    + '_verify_rollback.log')

        '''db_conn_str'''
        self.db_conn_str = db_connection_string

        '''oracle. Jenkins is linux2'''

        if sys.platform == 'linux2':
            print 'linux'
            self.oracle = '$ORACLE_HOME/bin/sqlplus'
        else:
            self.oracle = 'sqlplus'

        self.results = results

        self.db_name = self.db_conn_str.split('@')[-1]

        '''Status of sql script'''
        self.sql_error = False

        self.dmlpath = dmlpath
        self.backout_path = backout_path
        self.validation_path = validation_path

        self.CONFIG_ARGLIST = [
            'set echo on\n',
            '@' + self.dmlpath,
            'rollback;']

        self.BACKOUT_ARGLIST = [
            'set echo on\n',
            '@' + self.dmlpath,
            '@' + backout_path,
            '@' + self.dmlpath,
            'rollback;']

        self.timestamp = 'todo'

    def cursor(self):
        db_conn = cx_Oracle.Connection(self.db_conn_str)
        return db_conn.cursor()

    def add_rollback(self):
        """
        add append 'rollback' to end of config_file
        """
        with open(self.dmlpath, 'a') as f:
            f.write('\nrollback;')

    def sqlplus_comm(self, arg_list, logfile):
        '''Create sqlplus session, write command'''

        with open(logfile, 'a') as f:
            session = subprocess.Popen(
                self.oracle,
                stdin=subprocess.PIPE,
                stdout=f,
                stderr=subprocess.PIPE,
                shell=True)

            arg_list.insert(0, self.db_conn_str)
            for arg in arg_list:
                session.stdin.write(arg + '\n')
            session.communicate()

        '''Is there an error?'''
        with open(logfile, 'r') as f:
            s = f.read()
            if self.errors_in_sql(s):
                self.sql_error = True
        return s

    def run(self, arglist, logfile):
        '''
        Return log string
        Execute sql file, update sql_error status and generate log
        '''

        header = 'Connecting to sqlplus and executing:\n'
        status = header + '\n'.join(arg for arg in arglist)
        border = '*' * max(len(arg) for arg in arglist)
        print border
        print status
        print border + '\n'
        return self.sqlplus_comm(arglist, logfile)

    def run_sql(self):
        """
        Execute config_file
        If no errors create backout
        """
        self.run(self.CONFIG_ARGLIST, self.sqlplus_verify_logfile)

        '''If problem with dml: stop'''
        if self.sql_error:
            with open(self.sqlplus_verify_logfile, 'a') as f:
                f.write('{}'.format(Exception(self.error_report)))
            print 'Errors found verifying the dml script. Exiting'
            print 'Check {}'.format(self.sqlplus_verify_logfile)
            exit()
        else:
            '''
            - No errors after running dml script
            - create rollback.sql
            '''
            print 'no errors found in configuration.sql'
            print 'creating backout and validation'
            cd = self.results
            
            #Need to create a copy of the dictionary here before backout class masses it up
            cdv = ShadowCopyOfConfigDict(cd)
            
            Backout(self.backout_path, cd).create_backout()
            print 'backout created'
            
            #Passing shadow copy of the ConfigDict
            ValidationScript(self.validation_path, cdv).create_validation()
            print 'validation script created'
            '''validate backout'''
            print 'validating backout'

            self.run(self.BACKOUT_ARGLIST,
                     self.sqlplus_backout_logfile)

            if self.sql_error:
                f = os.path.basename(dmlpath)
                s = '{}\n{}\n{}\n{}\n{}\n{}'.format(
                    'Conflict exists in {}'.format(f),
                    '(backout.sql can not successfully revert it',
                    'Note: Nothing deployed to the database',
                    '\nSee {}'.format(self.sqlplus_backout_logfile),
                    '\nNote 2: Possibly the dml script contains statements',
                    '\nwhich update and delete rows from the same table')

                with open(self.sqlplus_backout_logfile, 'a') as f:
                    f.write('{}\n{}'.format(s, Exception(self.error_report)))
                print 'Errors found testing the backout script.'
                print 'check {}'.format(self.sqlplus_backout_logfile)

    def errors_in_sql(self, sql):
        """
        Raise an exception if error found when running config_file.
        """
        self.errors = False
        self.error_report = ''
        if 'ORA-' in sql:
            s = "Oracle Error in file: {} Line: {}".format(
                os.path.basename(self.dmlpath),
                self.results.current_line_num)
            self.errors = True
            self.error_report += s + '\n'
        if '\n0 rows updated' in sql:
            s = "'0 rows updated' found in logfile"
            self.errors = True
            self.error_report += s + '\n'

        if '\n0 rows selected' in sql:
            s = "'0 rows selected' found in logfile"
            self.errors = True
            self.error_report += s + '\n'
        return self.errors

    def main(self):
        self.run_sql()

#Modification to generate validation script on the fly
class ValidationScript():
    """
    Create Validation Script for production verifications
    """
    def __init__(self, validation_path, configdict):
        self.cd = configdict
        self.validation_path = validation_path
        self.update_deletes = {}
        self.update_inserts = {}
        self.inserts = {}
        self.deletes = {}
        self.delete_trailer_string = '\n -- BELOW ARE THE INSERTS THAT SHOULD RETURN no rows selected (CREATED FROM DELETES) \n';

    def create_validation(self):
        """
        Turn inserts or updates etc. into select & delete statements
        """

        '''
        handle updates
        '''
        def update_backout_deletes():
            line_nums = list(self.cd.update_line_nums)
            tabs = self.cd.update_tables
            updates = self.cd.updates
            # Create deletes
            for table in self.cd.post_update:
                tab = tabs[0]
                l = line_nums[0]
                u = updates[0]

                delete_root = 'SELECT * FROM' + ' ' + tab + ' ' + 'WHERE' + ' '
                self.update_deletes[line_nums[0]] = []
                for row in table:
                    str_ = ' AND '.join(
                        self.delete_vals(col, val) for col, val in row)
                    self.update_deletes[l].append(delete_root + str_ + ';')
                line_nums = line_nums[1:]
                tabs = tabs[1:]
                u = u[1:]

        def update_backout_inserts():
            # Create inserts
            line_nums = list(self.cd.update_line_nums)
            tabs = self.cd.update_tables
            updates = self.cd.updates
            for table in self.cd.pre_update:
                tab = tabs[0]
                l = line_nums[0]
                u = updates[0]
                insert_root = 'insert into' + ' ' + tab + ' '
                self.update_inserts[line_nums[0]] = []
                for row in table:
                    cols = '(' + ', '.join(col[0] for col in row) + ')'
                    vals = '(' + ', '.join(
                        self.insert_vals(val) for row, val in row) + ')'
                    self.update_inserts[l].append(
                        insert_root
                        + cols
                        + ' '
                        + 'values'
                        + ' '
                        + vals
                        + ';')
                tabs = tabs[1:]
                line_nums = line_nums[1:]
                u = u[1:]

        update_backout_deletes()
        update_backout_inserts()

        def inserts():
            # 2 inserts makes 2 deletes
            tables = self.cd.insert_tables
            line_nums = list(self.cd.insert_line_nums)
            for table in self.cd.inserts:
                l = line_nums[0]
                self.deletes[l] = []
                t = tables[0]
                result = ' and '.join(
                    self.delete_vals(col, val)
                    for col, val in table)
                delete = 'SELECT * FROM {} WHERE {};'.format(
                    t, result)
                self.deletes[l].append(delete)
                tables = tables[1:]
                line_nums = line_nums[1:]
        inserts()

        def deletes():
            # 1 delete makes many inserts
            tables = self.cd.delete_tables
            line_nums = list(self.cd.delete_line_nums)
            for table in self.cd.deletes:
                t = tables[0]
                l = line_nums[0]
                self.inserts[l] = []
                for row in table:
                    t = tables[0]
                    
                    cols = '(' + ', '.join(col[0] for col in row) + ')'
                    vals = '(' + ', '.join(self.insert_vals(val) for col, val in row) + ')'
                    insert = 'insert into {} {} values {};'.format(t, cols, vals)
                

                
                    select = 'SELECT * FROM {} WHERE '.format(t)
                
                    for col, val in row:
                        if val == None:
                            statement = col +' = '+ "'"+""+"'"+" AND "
                        else:
                            statement = col +' = '+ "'"+val+"'"+" AND "
                        
                        select += statement
                    
                    select = select[:-5]+';'
                    
                    #print(select)
                    self.inserts[l].append(select)
                    
                    
                tables = tables[1:]
                line_nums = line_nums[1:]
        deletes()

        with open(self.validation_path, 'w') as b:
            
            for line_num in self.cd.line_list: # Core modification
                if line_num in self.cd.update_line_nums:
                    line_num_format = "{} {}\n{} {}\n".format(
                        '--Line number:',
                        str(line_num),
                        '-- (EACH BELOW SELECT SHOULD RETURN 1 ROW) Created from - ',
                        str(self.cd.updates.pop(0)))
                    #b.write(line_num_format)
                    #b.write("\n".join([
                        #delete for delete in
                        #self.update_deletes[line_num]])
                        #+ '\n\n')
                    #b.write("\n".join([
                        #insert for insert in
                        #self.update_inserts[line_num]]) + '\n\n')
                elif line_num in self.cd.insert_line_nums:
                    line_num_format = "{} {}\n{} {}\n".format(
                        '--Line number:',
                        str(line_num),
                        '--  (BELOW SELECT SHOULD RETURN 1 ROW) Created from - ',
                        str(self.cd.insert_statements.pop(0)))
                    b.write(line_num_format)
                    b.write("\n".join([
                        delete for delete in
                        self.deletes[line_num]]) + '\n\n')
                elif line_num in self.cd.delete_line_nums:
                    line_num_format = "{} {}\n{} {}\n".format(
                        '--Line number:', str(line_num),
                        '--  (EACH BELOW SELECT SHOULD RETURN 0 ROWS) Created from - ', str(self.cd.delete_statements.pop(0)))
                    #b.write(line_num_format)
                    #b.write("\n".join([
                        #insert for insert in self.inserts[line_num]]) + '\n\n')
                    self.delete_trailer_string += line_num_format + "\n".join([insert for insert in self.inserts[line_num]]) + '\n\n'
                
            b.write(self.delete_trailer_string)

    def delete_vals(self, col, val):
        return ("{column} is NULL".format(column=col)
                if val is None else "{column} = '{value}'".format(
                    column=col, value=val))

    def insert_vals(self, val):
        return "{}".format('NULL') if val is None else "'{}'".format(val)

    def main(self):
        self.create_validation()

class ShadowCopyOfConfigDict():
    def __init__(self, configdict):
        self.actual_tables = list(configdict.actual_tables)
        self.line_list = list(configdict.line_list)
        self.updates = list(configdict.updates)
        self.inserts = list(configdict.inserts)
        self.deletes = list(configdict.deletes)
        self.update_line_nums = list(configdict.update_line_nums)
        self.update_tables = list(configdict.update_tables)
        self.insert_tables = list(configdict.insert_tables)
        self.delete_tables = list(configdict.delete_tables)
        self.pre_update = list(configdict.pre_update)
        self.post_update = list(configdict.post_update)
        self.insert_statements = list(configdict.insert_statements)
        self.delete_statements = list(configdict.delete_statements)
        self.insert_line_nums = list(configdict.insert_line_nums)
        self.delete_line_nums = list(configdict.delete_line_nums)
        self.del_or_up = list(configdict.del_or_up)



class Backout():
    """
    Create 'delete.txt' & 'select.txt' scripts
    """
    def __init__(self, backout_path, configdict):
        self.cd = configdict
        self.backout_path = backout_path
        self.update_deletes = {}
        self.update_inserts = {}
        self.inserts = {}
        self.deletes = {}

    def create_backout(self):
        """
        Turn inserts or updates etc. into select & delete statements
        """

        '''
        handle updates
        '''
        
        def update_backout_deletes():
            line_nums = list(self.cd.update_line_nums)
            tabs = self.cd.update_tables
            updates = self.cd.updates
            # Create deletes
            for table in self.cd.post_update:
                tab = tabs[0]
                l = line_nums[0]
                u = updates[0]

                delete_root = 'delete from' + ' ' + tab + ' ' + 'where' + ' '
                self.update_deletes[line_nums[0]] = []
                for row in table:
                    str_ = ' and '.join(
                        self.delete_vals(col, val) for col, val in row)
                    self.update_deletes[l].append(delete_root + str_ + ';')
                line_nums = line_nums[1:]
                tabs = tabs[1:]
                u = u[1:]

        def update_backout_inserts():
            # Create inserts
            line_nums = list(self.cd.update_line_nums)
            tabs = self.cd.update_tables
            updates = self.cd.updates
            for table in self.cd.pre_update:
                tab = tabs[0]
                l = line_nums[0]
                u = updates[0]
                insert_root = 'insert into' + ' ' + tab + ' '
                self.update_inserts[line_nums[0]] = []
                for row in table:
                    cols = '(' + ', '.join(col[0] for col in row) + ')'
                    vals = '(' + ', '.join(
                        self.insert_vals(val) for row, val in row) + ')'
                    self.update_inserts[l].append(
                        insert_root
                        + cols
                        + ' '
                        + 'values'
                        + ' '
                        + vals
                        + ';')
                tabs = tabs[1:]
                line_nums = line_nums[1:]
                u = u[1:]

        update_backout_deletes()
        update_backout_inserts()

        def inserts():
            # 2 inserts makes 2 deletes
            tables = self.cd.insert_tables
            line_nums = list(self.cd.insert_line_nums)
            for table in self.cd.inserts:
                l = line_nums[0]
                self.deletes[l] = []
                t = tables[0]
                result = ' and '.join(
                    self.delete_vals(col, val)
                    for col, val in table)
                delete = 'delete from {} where {};'.format(
                    t, result)
                self.deletes[l].append(delete)
                tables = tables[1:]
                line_nums = line_nums[1:]
        inserts()

        def deletes():
            # 1 delete makes many inserts
            tables = self.cd.delete_tables
            line_nums = list(self.cd.delete_line_nums)
            for table in self.cd.deletes:
                t = tables[0]
                l = line_nums[0]
                self.inserts[l] = []
                for row in table:
                    t = tables[0]
                    cols = '(' + ', '.join(col[0] for col in row) + ')'
                    vals = '(' + ', '.join(self.insert_vals(val)
                                           for col, val in row) + ')'
                    insert = 'insert into {} {} values {};'.format(
                        t, cols, vals)
                    self.inserts[l].append(insert)
                tables = tables[1:]
                line_nums = line_nums[1:]
        deletes()

        with open(self.backout_path, 'w') as b:
            for line_num in reversed(self.cd.line_list):
                if line_num in self.cd.update_line_nums:
                    line_num_format = "{} {}\n{} {}\n".format(
                        '--Line number:',
                        str(line_num),
                        '--',
                        str(self.cd.updates.pop(0)))
                    b.write(line_num_format)
                    b.write("\n".join([
                        delete for delete in
                        self.update_deletes[line_num]])
                        + '\n\n')
                    b.write("\n".join([
                        insert for insert in
                        self.update_inserts[line_num]]) + '\n\n')
                elif line_num in self.cd.insert_line_nums:
                    line_num_format = "{} {}\n{} {}\n".format(
                        '--Line number:',
                        str(line_num),
                        '--',
                        str(self.cd.insert_statements.pop(0)))
                    b.write(line_num_format)
                    b.write("\n".join([
                        delete for delete in
                        self.deletes[line_num]]) + '\n\n')
                elif line_num in self.cd.delete_line_nums:
                    line_num_format = "{} {}\n{} {}\n".format(
                        '--Line number:', str(line_num),
                        '--', str(self.cd.delete_statements.pop(0)))
                    b.write(line_num_format)
                    b.write("\n".join([
                        insert for insert in self.inserts[line_num]]) + '\n\n')

    def delete_vals(self, col, val):
        return ("{column} is NULL".format(column=col)
                if val is None else "{column} = '{value}'".format(
                    column=col, value=val))

    def insert_vals(self, val):
        return "{}".format('NULL') if val is None else "'{}'".format(val)

    def main(self):
        self.create_backout()


def get_db_user():
    user = raw_input('db username:')
    return user


def get_db():
    db = raw_input('db SID:')
    return db


def getpw():
    pw = getpass.getpass('Password:', stream=None)
    return pw


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='PROG',
        description='')
    parser.add_argument('-dml',
                        nargs=1,
                        help='path to the dml script')
    args = parser.parse_args()
    dmlpath = args.dml[0]
    if not os.path.exists(dmlpath):
        print 'Path, {}, does not exist'.format(dmlpath)
        exit()

    #dbuser = get_db_user()
    #db = get_db()
    #pw = getpw()

    dbuser = 'SYSIMP_UTIL'
    db = 'SYSIMP'
    pw = 'sysimp'
    
    db_connection_string = '{}/{}@{}'.format(dbuser, pw, db)

    timestamp = datetime.datetime.utcnow().strftime('%H%M%S_%Y_%d%B')
    cx_Oracle_logfile = '{}_{}_cx_Oracle'.format(timestamp, db)
    sqlplus_logfile = '{}_{}_sqlplus'.format(timestamp, db)

    config_dict = ConfigDict(db_connection_string, dmlpath, cx_Oracle_logfile)


    config_dict.validate_config(['commit', 'disable'])
    config_dict.process_config()
    
    

    backout_path = os.path.join(this_dir, timestamp + '_' + db + '_rollback.sql')
    validation_path = os.path.join(this_dir, timestamp + '_' + db + '_validation.sql')

    if os.path.exists(config_dict.cx_Oracle_logfile):
        print 'Oracle errors in cx_Oracle log'
    else:
        print 'No Oracle database errors'
        print '\nRunning configuration into sqlplus'
        db = Db(dmlpath, backout_path, config_dict, sqlplus_logfile,
                db_connection_string,validation_path)
        db.main()
