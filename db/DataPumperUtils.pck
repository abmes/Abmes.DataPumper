create or replace package DataPumperUtils authid CURRENT_USER is

  procedure OpenFileForRead(AFileName in Varchar2, ADirectoryName in Varchar2:= null);
  procedure OpenFileForWrite(AFileName in Varchar2, ADirectoryName in Varchar2:= null);
  procedure CloseFile;
  
  procedure ReadFromFile(AByteCount in Number, AData out nocopy Raw);
  function GetBlobFromFile(AByteCount in Number) return Blob;
  procedure WriteToFile(AData in Raw);

  function FileExists(AFileName in Varchar2, ADirectoryName in Varchar2:= null) return Boolean;
  function FileExistsNum(AFileName in Varchar2, ADirectoryName in Varchar2:= null) return Number;
  function FileLength(AFileName in Varchar2, ADirectoryName in Varchar2:= null) return Number;
  procedure DeleteFile(AFileName in Varchar2, ADirectoryName in Varchar2:= null);

  -- synchronous
  function ExportSchema(ASchemaName in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null, ADumpFileSize in Varchar2:= null) return Varchar2;
  function ImportSchema(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null) return Varchar2;

  -- asynchronous
  procedure StartExportSchema(ASchemaName in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null, ADumpFileSize in Varchar2:= null);
  procedure StartImportSchema(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null);
  procedure DoStartImportSchema(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null);

end;
/
create or replace package body DataPumperUtils is

  ImportJobName constant Varchar2(200):= 'DATAPUMPER_IMPORT_JOB';
  ImportJobComment constant Varchar2(200):= 'DataPumper Import Job';
  DefaultDirectoryName constant Varchar2(200):= 'DATA_PUMP_DIR';
  MaxChunkSize constant Number := 32000;  -- used in GetBlobFromFile

  FileHandle UTL_FILE.file_type:= null;
  IsFileHandleInUse Boolean:= False;

  function FileExists(AFileName in Varchar2, ADirectoryName in Varchar2:= null) return Boolean is
    LDirectoryName Varchar2(200);
    LFileExists Boolean;
    LLength Number;
    LDummy Number;
  begin
    if (ADirectoryName is null) then
      LDirectoryName:= DefaultDirectoryName;
    else
      LDirectoryName:= ADirectoryName;
    end if;
    
    UTL_FILE.fgetattr(LDirectoryName, AFileName, LFileExists, LLength, LDummy);
    
    if LFileExists then
      return True;
    end if;
    
    return False;
  end;

  
  function FileExistsNum(AFileName in Varchar2, ADirectoryName in Varchar2:= null) return Number is
  begin
    if FileExists(AFileName, ADirectoryName) then
      return 1;
    end if;
    
    return 0;
  end;


  function FileLength(AFileName in Varchar2, ADirectoryName in Varchar2:= null) return Number is
    LDirectoryName Varchar2(200);
    LFileExists Boolean;
    LLength Number;
    LDummy Number;
  begin
    if (ADirectoryName is null) then
      LDirectoryName:= DefaultDirectoryName;
    else
      LDirectoryName:= ADirectoryName;
    end if;
    
    UTL_FILE.fgetattr(LDirectoryName, AFileName, LFileExists, LLength, LDummy);
    
    if LFileExists then
      return LLength;
    end if;
    
    return null;
  end;
    

  procedure DeleteFile(AFileName in Varchar2, ADirectoryName in Varchar2:= null) is
    LDirectoryName Varchar2(200);
  begin
    if (ADirectoryName is null) then
      LDirectoryName:= DefaultDirectoryName;
    else
      LDirectoryName:= ADirectoryName;
    end if;
    
    UTL_FILE.fremove(LDirectoryName, AFileName);
  end;


  procedure OpenFileForRead(AFileName in Varchar2, ADirectoryName in Varchar2:= null) is
    LDirectoryName Varchar2(200);
  begin
    if (ADirectoryName is null) then
      LDirectoryName:= DefaultDirectoryName;
    else
      LDirectoryName:= ADirectoryName;
    end if;
      
    if IsFileHandleInUse then
      raise_application_error(-20000, 'DataPumperUtils: Only one open file allowed.');
    end if;
    
    FileHandle:= UTL_FILE.fopen(LDirectoryName, AFileName, 'rb');
    IsFileHandleInUse:= True;
  end;
  
  
  procedure OpenFileForWrite(AFileName in Varchar2, ADirectoryName in Varchar2:= null) is
    LDirectoryName Varchar2(200);
  begin
    if (ADirectoryName is null) then
      LDirectoryName:= DefaultDirectoryName;
    else
      LDirectoryName:= ADirectoryName;
    end if;
    
    if IsFileHandleInUse then
      raise_application_error(-20000, 'DataPumperUtils: Only one open file allowed.');
    end if;

    if FileExists(AFileName, LDirectoryName) then
      UTL_FILE.fremove(LDirectoryName, AFileName);
    end if;

    FileHandle:= UTL_FILE.fopen(LDirectoryName, AFileName, 'wb');
    IsFileHandleInUse:= True;
  end;
  
  
  procedure CloseFile is
  begin
    if IsFileHandleInUse then
      UTL_FILE.fclose(FileHandle);
      FileHandle:= null;
      IsFileHandleInUse:= False;
    end if;
  end;

  
  procedure ReadFromFile(AByteCount in Number, AData out nocopy Raw) is
  begin
    begin
      UTL_FILE.get_raw(FileHandle, AData, AByteCount);
    exception
      when NO_DATA_FOUND then AData:= null;
    end;
  end;
  
  
  function GetBlobFromFile(AByteCount in Number) return Blob is
    ResultData Blob;
    DataChunk Long Raw;
    ChunkSize Number;
    ChunkReadSize Number;
    ReadByteCount Number;
    RemainingByteCount Number;
  begin
    DBMS_LOB.createtemporary(ResultData, True);  -- call / transaction, false?
    
    ReadByteCount := 0;
    RemainingByteCount := AByteCount;
    
    while (RemainingByteCount > 0) loop
      
      if (RemainingByteCount < MaxChunkSize) then
        ChunkSize := remainingByteCount;
      else
        ChunkSize := MaxChunkSize;
      end if;

      begin
        UTL_FILE.get_raw(FileHandle, DataChunk, ChunkSize);
        ChunkReadSize := UTL_RAW.length(DataChunk);
      exception
        when NO_DATA_FOUND then ChunkReadSize := 0;
      end;
      
      if (ChunkReadSize = 0) then
        Exit;
      end if;        
      
      DBMS_LOB.append(ResultData, DataChunk);

      ReadByteCount := ReadByteCount + ChunkReadSize;
      RemainingByteCount := RemainingByteCount - ChunkReadSize;
      
    end loop;
    
    return ResultData;
  end; 
  
  
  procedure WriteToFile(AData in Raw) is
  begin
    UTL_FILE.put_raw(FileHandle, AData, True);
  end;


  function DoStartExportSchemaJob(ASchemaName in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null, ADumpFileSize in Varchar2) return Number is
    DataPumpJobHandle Number;
  begin
    DataPumpJobHandle:=
      DBMS_DATAPUMP.open(
        operation => 'EXPORT',
        job_mode => 'SCHEMA');
      
    DBMS_DATAPUMP.add_file(DataPumpJobHandle, ADumpFileName, ADirectoryName, filesize => ADumpFileSize, reusefile => 1);
    DBMS_DATAPUMP.add_file(DataPumpJobHandle, ALogFileName, ADirectoryName, filetype => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE, reusefile => 1);

    DBMS_DATAPUMP.metadata_filter(DataPumpJobHandle,'SCHEMA_EXPR','= ' || '''' || Upper(ASchemaName) || '''');
    DBMS_DATAPUMP.metadata_filter(
      DataPumpJobHandle,
      'EXCLUDE_PATH_LIST',
      '''VIEW'',''PACKAGE'',''FUNCTION'',''PROCEDURE'',''TRIGGER'',''LIBRARY'',''SYNONYM'''); -- ako napisha slednite, ne raboti posle importa: ,''USER'',''SYSTEM_GRANT'',''ROLE_GRANT'',''DEFAULT_ROLE''

    DBMS_DATAPUMP.set_parameter(
      handle => DataPumpJobHandle,
      name => 'METRICS',
      value => 1
    );
    
    DBMS_DATAPUMP.set_parameter(
      handle => DataPumpJobHandle,
      name => 'FLASHBACK_SCN',
      value => timestamp_to_scn(systimestamp)
    );

    DBMS_DATAPUMP.log_entry(
        handle => DataPumpJobHandle,
        message => 'Job starting at '||to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
    );      

    DBMS_DATAPUMP.start_job(DataPumpJobHandle);

    return DataPumpJobHandle;
  end;


  function ExportSchema(ASchemaName in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null, ADumpFileSize in Varchar2:= null) return Varchar2 is
    DataPumpJobHandle Number;
    DataPumpJobFinalState Varchar2(4000);
  begin
    DataPumpJobHandle:= DoStartExportSchemaJob(ASchemaName, ADumpFileName, ALogFileName, ADirectoryName, ADumpFileSize);
    
    DBMS_DATAPUMP.wait_for_job(
      handle => DataPumpJobHandle,
      job_state => DataPumpJobFinalState);
     
    return DataPumpJobFinalState;    
  end;


  procedure StartExportSchema(ASchemaName in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null, ADumpFileSize in Varchar2:= null) is
    DataPumpJobHandle Number;
  begin
    DataPumpJobHandle:= DoStartExportSchemaJob(ASchemaName, ADumpFileName, ALogFileName, ADirectoryName, ADumpFileSize);
    
    DBMS_DATAPUMP.detach(DataPumpJobHandle);
  end;


  function UserExists(AUserName in Varchar2) return Boolean is
    DoesUserExist Number;
  begin
    select
      Sign(Count(*))
    into
      DoesUserExist
    from
      ALL_USERS u
    where
      (u.USERNAME = Upper(AUserName));
  
    return (DoesUserExist = 1);
  end;


  procedure CreateUser(AUserName in Varchar2, APassword in Varchar2) is
  begin
    execute immediate 'create user ' || Upper(AUserName) || ' identified by "' || APassword || '"';
    
    execute immediate 'grant resource, connect to ' || Upper(AUserName);
    execute immediate 'grant unlimited tablespace to ' || Upper(AUserName);
    execute immediate 'grant create table to ' || Upper(AUserName);
    execute immediate 'grant create sequence to ' || Upper(AUserName);
    execute immediate 'grant create synonym to ' || Upper(AUserName);
    execute immediate 'grant create view to ' || Upper(AUserName);    
    execute immediate 'alter user ' || Upper(AUserName) || ' default role all';
  end;


  procedure DropUser(AUserName in Varchar2) is
  begin
    execute immediate 'drop user ' || Upper(AUserName) || ' cascade';
  end;


  function DoStartImportSchemaDataPumpJob(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null) return Number is
    DataPumpJobHandle Number;
  begin
    DataPumpJobHandle:=
      DBMS_DATAPUMP.open(
        operation => 'IMPORT',
        job_mode => 'SCHEMA');
      
    DBMS_DATAPUMP.add_file(DataPumpJobHandle, ADumpFileName, ADirectoryName);
    DBMS_DATAPUMP.add_file(DataPumpJobHandle, ALogFileName, ADirectoryName, filetype => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE, reusefile => 1);

    DBMS_DATAPUMP.metadata_filter(
      DataPumpJobHandle,
      'EXCLUDE_PATH_LIST',
      '''USER'',''SYSTEM_GRANT'',''ROLE_GRANT'',''DEFAULT_ROLE''');

    DBMS_DATAPUMP.set_parameter(
      handle => DataPumpJobHandle,
      name => 'METRICS',
      value => 1
    );

    DBMS_DATAPUMP.metadata_remap(
      DataPumpJobHandle,
      'REMAP_SCHEMA',
      Upper(AFromSchemaName),
      Upper(AToSchemaName));


    if UserExists(AToSchemaName) then
      
      DBMS_DATAPUMP.log_entry(
          handle => DataPumpJobHandle,
          message => 'Schema user ' || AToSchemaName || ' already exists.'
      );
      
      DBMS_DATAPUMP.log_entry(
          handle => DataPumpJobHandle,
          message => 'Drop user ' || AToSchemaName || ' started at ' ||  to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
      );
      
      begin
        DropUser(AToSchemaName);
      exception
        when others then
          DBMS_DATAPUMP.log_entry(
              handle => DataPumpJobHandle,
              message => 'Could not drop user ' || AToSchemaName || '. Error: ' || SQLCODE || ' ' || SQLERRM
          );
          raise;
      end;
      
      DBMS_DATAPUMP.log_entry(
          handle => DataPumpJobHandle,
          message => 'Drop user ' || AToSchemaName || ' finished successfully at ' ||  to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
      );
      
    end if;
  
    DBMS_DATAPUMP.log_entry(
        handle => DataPumpJobHandle,
        message => 'Create user ' || AToSchemaName || ' started at ' ||  to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
    );
      
    begin
      CreateUser(AToSchemaName, AToSchemaPassword);
    exception
      when others then
        DBMS_DATAPUMP.log_entry(
            handle => DataPumpJobHandle,
            message => 'Could not create user ' || AToSchemaName || '. Error: ' || SQLCODE || ' ' || SQLERRM
        );
        raise;
    end;
    
    DBMS_DATAPUMP.log_entry(
        handle => DataPumpJobHandle,
        message => 'Create user ' || AToSchemaName || ' finished successfully at ' ||  to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
    );

    DBMS_DATAPUMP.log_entry(
        handle => DataPumpJobHandle,
        message => 'Import job starting at ' || to_char(sysdate, 'YYYY-MM-DD HH24:MI:SS')
    );      

    DBMS_DATAPUMP.start_job(DataPumpJobHandle);
    
    return DataPumpJobHandle;
  end;


  function ImportSchema(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null) return Varchar2 is
    DataPumpJobHandle Number;
    DataPumpJobFinalState Varchar2(4000);
  begin
    DataPumpJobHandle:= DoStartImportSchemaDataPumpJob(AFromSchemaName, AToSchemaName, AToSchemaPassword, ADumpFileName, ALogFileName, ADirectoryName);

    DBMS_DATAPUMP.wait_for_job(
      handle => DataPumpJobHandle,
      job_state => DataPumpJobFinalState);
     
    return DataPumpJobFinalState;    
  end;


  procedure DoStartImportSchema(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2) is
    DataPumpJobHandle Number;
  begin
    begin
      DataPumpJobHandle:= DoStartImportSchemaDataPumpJob(AFromSchemaName, AToSchemaName, AToSchemaPassword, ADumpFileName, ALogFileName, ADirectoryName);
      DBMS_DATAPUMP.detach(DataPumpJobHandle);
    exception
      when others then
        null;
    end;
  end;


  function JobExists(AJobName in Varchar2) return Boolean is
    cnt Number(10);
  begin
    select
      Count(*)
    into
      cnt
    from
      user_scheduler_jobs j
    where
      (j.job_name = AJobName);
      
    return (cnt > 0);
  end;
  

  procedure StartImportSchema(AFromSchemaName in Varchar2, AToSchemaName in Varchar2, AToSchemaPassword in Varchar2, ADumpFileName in Varchar2, ALogFileName in Varchar2, ADirectoryName in Varchar2:= null) is
  begin
    if JobExists(ImportJobName) then
      DBMS_SCHEDULER.drop_job(job_name => ImportJobName, force => true);      
    end if;
  
    DBMS_SCHEDULER.create_job (
        job_name        => ImportJobName,
        job_type        => 'STORED_PROCEDURE',
        job_action      => 'DataPumperUtils.DoStartImportSchema',
        number_of_arguments => 6,
        start_date      => null,
        repeat_interval => null,
        end_date        => null,
        enabled         => false,
        auto_drop       => true,       
        comments        => ImportJobComment);

    dbms_scheduler.set_attribute(ImportJobName, 'max_runs', 1);

    dbms_scheduler.set_job_argument_value(ImportJobName, 1 , AFromSchemaName);
    dbms_scheduler.set_job_argument_value(ImportJobName, 2 , AToSchemaName);
    dbms_scheduler.set_job_argument_value(ImportJobName, 3 , AToSchemaPassword);
    dbms_scheduler.set_job_argument_value(ImportJobName, 4 , ADumpFileName);
    dbms_scheduler.set_job_argument_value(ImportJobName, 5 , ALogFileName);
    dbms_scheduler.set_job_argument_value(ImportJobName, 6 , ADirectoryName);

    DBMS_SCHEDULER.enable(ImportJobName);    
  end;
  
end;
/
