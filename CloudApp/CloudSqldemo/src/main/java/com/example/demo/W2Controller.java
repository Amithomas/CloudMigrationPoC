package com.example.demo;



import java.util.List;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@CrossOrigin(origins = "*", allowedHeaders = "*")
@RestController
public class W2Controller {
	@Autowired
	private CustomerDetailsRepository customerDetailsRepository;
	
	@CrossOrigin
	@RequestMapping(value="/", method = RequestMethod.GET)
	public String checkService (){
		return "All OK!";
	}
	
	@CrossOrigin
	@RequestMapping(value="/getAllValues", method = RequestMethod.GET)
	public List<CustomerDetails> getAllValues (){
		return customerDetailsRepository.findAll();
	}
	
    
    @CrossOrigin
	@RequestMapping(value="/postCD", method = RequestMethod.GET)
	public List<CustomerDetails> PostCD (){
    	CustomerDetails w2Customer= new CustomerDetails();
    	w2Customer.setCustomerId(1);
    	w2Customer.setCustomerName("Satheesh Unnikrishnan");
    	w2Customer.setRegEmail("Satheesh.Unnikrishnan@equifax.com");
    	w2Customer.setRegMob("+17329939672");
    	w2Customer.setRegStatus(1);
		customerDetailsRepository.save(w2Customer);
		
		CustomerDetails w2Customer1= new CustomerDetails();
    	w2Customer1.setCustomerId(2);
    	w2Customer1.setCustomerName("Amith Thomas");
    	w2Customer1.setRegEmail("amithomas95@gmail.com");
    	w2Customer1.setRegMob("+12057750342");
    	w2Customer1.setRegStatus(1);
    	customerDetailsRepository.save(w2Customer1);
    	
    	CustomerDetails w2Customer2= new CustomerDetails();
    	w2Customer2.setCustomerId(3);
    	w2Customer2.setCustomerName("Mathews");
    	w2Customer2.setRegEmail("Mathews.Abraham@equifax.com");
    	w2Customer2.setRegMob("+919048620256");
    	w2Customer2.setRegStatus(1);
    	customerDetailsRepository.save(w2Customer2);
    	
    	CustomerDetails w2Customer3= new CustomerDetails();
    	w2Customer3.setCustomerId(4);
    	w2Customer3.setCustomerName("Rajan");
    	w2Customer3.setRegEmail("Rajan.Kundoor@equifax.com");
    	w2Customer3.setRegMob("+919048611468");
    	w2Customer3.setRegStatus(0);
    	customerDetailsRepository.save(w2Customer3);
    	
    	
    	return customerDetailsRepository.findAll();
	}
    
    @CrossOrigin
	@RequestMapping(value="/insert", method = RequestMethod.POST)
	public CustomerDetails insertDB (@RequestBody CustomerDetails cd){
    	return customerDetailsRepository.save(cd);
    }
    
    @CrossOrigin
    @RequestMapping(value="/getById/{id}", method = RequestMethod.GET)
	public CustomerDetails insertDB (@PathVariable("id") int id){
    	return customerDetailsRepository.findBycustomerId(id);
    }
    
    
    
}
