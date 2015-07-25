package com.jzhou.hbase.dataload;

public enum HColumnEnum {
	  COL_JAN ("jan".getBytes()),
	  COL_FEB ("feb".getBytes()),
	  COL_MAR ("mar".getBytes()),
	  COL_APR ("apr".getBytes()),
	  COL_MAY ("may".getBytes()),
	  COL_JUN ("jun".getBytes()),
	  COL_JUL ("jul".getBytes()),
	  COL_AUG ("aug".getBytes()),
	  COL_SEP ("sep".getBytes()),
	  COL_OCT ("oct".getBytes()),
	  COL_NOV ("nov".getBytes()),
	  COL_DEC ("dec".getBytes());
	 
	  
	  private final byte[] columnName;
	  
	  HColumnEnum (byte[] column) {
	    this.columnName = column;
	  }

	  public byte[] getColumnName() {
	    return this.columnName;
	  }
	}
