package org.apache.pig.impl.io;

public class ResourceNotFoundException extends Exception
{
  private static final long serialVersionUID = 1L;

  private String resourceName;
  
  public ResourceNotFoundException(String resourceName)
  {
    this.resourceName = resourceName;
  }
  
  public String toString()
  {
    return "Resource '" + resourceName + "' does not exist";
  }
}
