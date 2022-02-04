package org.myorg.quickstart;

public class KafkaStreamSchema {
  public String job_id;
  public NodeConfigurationSchema parameters;

  public String getJob_id() {
    return job_id;
  }

  public void setJob_id(String job_id) {
    this.job_id = job_id;
  }

  public NodeConfigurationSchema getParameters() {
    return parameters;
  }

  public void setParameters(NodeConfigurationSchema parameters) {
    this.parameters = parameters;
  }

  @Override
  public String toString() {
    return "KafkaStreamSchema{" +
           "job_id='" + job_id + '\'' +
           ", parameters=" + parameters +
           '}';
  }
}
