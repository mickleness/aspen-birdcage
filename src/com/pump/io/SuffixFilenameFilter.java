/**
 * This software is released as part of the Pumpernickel project.
 * 
 * All com.pump resources in the Pumpernickel project are distributed under the
 * MIT License:
 * https://raw.githubusercontent.com/mickleness/pumpernickel/master/License.txt
 * 
 * More information about the Pumpernickel project is available here:
 * https://mickleness.github.io/pumpernickel/
 */
package com.pump.io;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ResourceBundle;

/** A file filter that is based on a suffix.
 * 
 */
public class SuffixFilenameFilter extends javax.swing.filechooser.FileFilter implements FilenameFilter, java.io.FileFilter {

	/** The method isDirectory() is never consulted: this accepts/rejects all files based only on their suffix. */
	public static final int POLICY_IGNORE_DIRECTORY_STATUS = 0;
	/** All directories are accepted by this filter. */
	public static final int POLICY_ACCEPT_ALL_DIRECTORIES = 1;
	/** All directories are rejected by this filter. */
	public static final int POLICY_REJECT_ALL_DIRECTORIES = 2;
	String[] suffix;
	String description;
	int directoryPolicy;
	
	/** Returns true if this object has the same policy directory and suffixes. */
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof SuffixFilenameFilter))
			return false;
		SuffixFilenameFilter sff = (SuffixFilenameFilter)obj;
		if(setEquals(suffix,sff.suffix)==false)
			return false;
		if(directoryPolicy!=sff.directoryPolicy)
			return false;
		if(description==null && sff.description==null)
			return true;
		return getDescription().equals(sff.getDescription());
	}
	
	/** Returns true if each array has the same elements, regardless of their order. */
	private static boolean setEquals(String[] array1,String[] array2) {
		if(array1.length!=array2.length)
			return false;
		for(int a = 0; a<array1.length; a++) {
			boolean hit = false;
			for(int b = 0; b<array2.length; b++) {
				if(array1[a].equals(array2[b])) {
					hit = true;
				}
			}
			if(hit==false)
				return false;
		}
		for(int a = 0; a<array2.length; a++) {
			boolean hit = false;
			for(int b = 0; b<array1.length; b++) {
				if(array2[a].equals(array1[b])) {
					hit = true;
				}
			}
			if(hit==false)
				return false;
		}
		return true;
	}

	/** This creates a new SuffixFilenameFilter.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffix a file name suffix to accept such as "foo" or ".foo".
	 * @param directoryPolicy must be <code>POLICY_ACCEPT_ALL_DIRECTORIES</code>,
	 * <code>POLICY_REJECT_ALL_DIRECTORIES</code> or <code>POLICY_IGNORE_DIRECTORY_STATUS</code>.
	 */
	public SuffixFilenameFilter(String suffix,int directoryPolicy) {
		this(new String[] {suffix}, directoryPolicy);
	}

	/** This creates a new SuffixFilenameFilter.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffix1 a file name suffix to accept such as "foo" or ".foo".
	 * @param suffix2 a file name suffix to accept such as "goo" or ".goo".
	 * @param directoryPolicy must be <code>POLICY_ACCEPT_ALL_DIRECTORIES</code>,
	 * <code>POLICY_REJECT_ALL_DIRECTORIES</code> or <code>POLICY_IGNORE_DIRECTORY_STATUS</code>.
	 */
	public SuffixFilenameFilter(String suffix1,String suffix2,int directoryPolicy) {
		this(new String[] {suffix1, suffix2}, directoryPolicy);
	}

	/** This creates a new SuffixFilenameFilter.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffix1 a file name suffix to accept such as "foo" or ".foo".
	 * @param suffix2 a file name suffix to accept such as "goo" or ".goo".
	 * @param suffix3 a file name suffix to accept such as "boo" or ".boo".
	 * @param directoryPolicy must be <code>POLICY_ACCEPT_ALL_DIRECTORIES</code>,
	 * <code>POLICY_REJECT_ALL_DIRECTORIES</code> or <code>POLICY_IGNORE_DIRECTORY_STATUS</code>.
	 */
	public SuffixFilenameFilter(String suffix1,String suffix2,String suffix3,int directoryPolicy) {
		this(new String[] {suffix1, suffix2, suffix3}, directoryPolicy);
	}
	
	/** This creates a new SuffixFilenameFilter.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffixes the suffixes this filter will accept such as "foo" or ".foo".
	 * @param directoryPolicy must be <code>POLICY_ACCEPT_ALL_DIRECTORIES</code>,
	 * <code>POLICY_REJECT_ALL_DIRECTORIES</code> or <code>POLICY_IGNORE_DIRECTORY_STATUS</code>.
	 */
	public SuffixFilenameFilter(String[] suffixes,int directoryPolicy) {
		if(!(directoryPolicy==POLICY_ACCEPT_ALL_DIRECTORIES ||
				directoryPolicy==POLICY_IGNORE_DIRECTORY_STATUS ||
				directoryPolicy==POLICY_REJECT_ALL_DIRECTORIES))
			throw new IllegalArgumentException("unrecognized policy argument");
		suffix = new String[suffixes.length];
		for(int a = 0; a<suffixes.length; a++) {
			suffix[a] = suffixes[a].toLowerCase();
			if(suffix[a].startsWith("."))
				suffix[a] = suffix[a].substring(1);
		}
		this.directoryPolicy = directoryPolicy;
	}
	
	/** This creates a new SuffixFilenameFilter that does not accept directories.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffix the file name suffix to accept such as "foo" or ".foo".
	 */
	public SuffixFilenameFilter(String suffix) {
		this(suffix, POLICY_REJECT_ALL_DIRECTORIES);
	}

	/** This creates a new SuffixFilenameFilter that does not accept directories.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffix1 a file name suffix to accept such as "foo" or ".foo".
	 * @param suffix2 a file name suffix to accept such as "goo" or ".goo".
	 */
	public SuffixFilenameFilter(String suffix1,String suffix2) {
		this(suffix1,suffix2,POLICY_REJECT_ALL_DIRECTORIES);
	}

	/** This creates a new SuffixFilenameFilter that does not accept directories.
	 * Note that if "*" is one of the suffixes:
	 * then only directories will be rejected by this filter.
	 * @param suffix1 a file name suffix to accept such as "foo" or ".foo".
	 * @param suffix2 a file name suffix to accept such as "goo" or ".goo".
	 * @param suffix3 a file name suffix to accept such as "boo" or ".boo".
	 */
	public SuffixFilenameFilter(String suffix1,String suffix2,String suffix3) {
		this(suffix1,suffix2,suffix3,POLICY_REJECT_ALL_DIRECTORIES);
	}
	
	/** This creates a new SuffixFilenameFilter that does not accept directories.
	 * 
	 * @param suffixes the suffixes to accept such as "foo" or ".foo".  Note that
	 * if "*" is one of the suffixes: then only directories will be rejected by this filter.
	 */
	public SuffixFilenameFilter(String... suffixes) {
		this(suffixes,POLICY_REJECT_ALL_DIRECTORIES);
	}
	
	/** Returns <code>true</code> if this filter accepts the argument.
	 * This will be determined by:
	 * <ul><li>Whether the file is a directory and what the <code>directoryPolicy</code>
	 * of this filter is.
	 * <li>Whether one of the accepted suffixes is "*"
	 * <li>Whether the argument name ends in one of the suffixes</ul>
	 */
	@Override
	public boolean accept(File file) {
		if(directoryPolicy==POLICY_ACCEPT_ALL_DIRECTORIES && file.isDirectory())
			return true;
		if(directoryPolicy==POLICY_REJECT_ALL_DIRECTORIES && file.isDirectory())
			return false;
		return isAcceptedName(file.getName());
	}
	
	/** This returns true if the name ends in one of the suffixes
	 * defined in this filter.  (Or this returns true if one of the
	 * suffixes in this filter is "*").
	 * 
	 * @param name
	 */
	public boolean isAcceptedName(String name) {
		name = name.toLowerCase();
		for(int a = 0; a<suffix.length; a++) {
			if(name.endsWith('.'+suffix[a]) || suffix[a].equals("*"))
				return true;
		}
		return false;
	}

	/** Assigns the description for this filter.
	 * If null, then a default description will be provided.
	 * @param description
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	
	@Override
	public String toString() {
		return "SuffixFilenameFilter[ \""+getDescription()+"\" ]";
	}

	/** If a specific description was provided: this
	 * returns the predefined description.  Otherwise this
	 * returns a string such as "GIF and JPEG files".
	 */
	@Override
	public String getDescription() {
		if(description!=null) return description;
		return "";
	}

	/** This defers to <code>accept(File)</code>. */
	public boolean accept(File dir, String name) {
		File newFile = new File(dir, name);
		return accept(newFile);
	}
}