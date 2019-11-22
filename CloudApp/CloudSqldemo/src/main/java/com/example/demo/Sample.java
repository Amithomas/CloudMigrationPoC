package com.example.demo;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "Sample")
public class Sample {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column
	private Integer customerId;
	
	@Column
	private String customerName;
	
	@Column 
	private String regMob;
	
	@Column
	private String regEmail;
	
	public Integer getCustomerId() {
		return customerId;
	}

	public void setCustomerId(Integer customerId) {
		this.customerId = customerId;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getRegMob() {
		return regMob;
	}

	public void setRegMob(String regMob) {
		this.regMob = regMob;
	}

	public String getRegEmail() {
		return regEmail;
	}

	public void setRegEmail(String regEmail) {
		this.regEmail = regEmail;
	}

	
	

}
