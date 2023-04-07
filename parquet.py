import pyarrow as pa
import pyarrow.parquet as pq

if __name__=="__main__":
  pq_array = pa.parquet.read_table("/Users/george/Downloads/test6dicom.parquet",memory_map=True)
  p_file= pq.ParquetFile("/Users/george/Downloads/test6dicom.parquet")
  #nfield=pa.field("mm",pa.string(),True, None)
  #pq_array.schema[5]="string"
  #pq_array.columns[5]
  #ncol=pa.chunked_array(list(pq_array.columns[4]), type=pa.int32())
  #pq_array.columns[4]=ncol
  #nschema=pa.schema([("tmp",pa.string()), ("tmp2", pa.string())])
  #nschema.set(4,nfield)
  new_schema=pq_array.schema.set(4, pa.field("InstanceCreationTime",pa.time32('ms')))


  #nschema.types[4]=pa.string()
  new_array=pq_array.cast(target_schema=new_schema)
  pa.parquet.write_table(new_array,'/Users/george/Documents/tryit_tbl.parquet')
  rr=1

  #pq_array['InstanceCreationTime'].cast(pa.string())
  rr=1




  k=1



