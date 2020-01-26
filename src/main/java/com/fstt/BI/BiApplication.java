package com.fstt.BI;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fstt.BI.Controllers.BiController;
import com.fstt.BI.Service.ArticleService;

@SpringBootApplication
public class BiApplication {

	public static void main(String[] args) {
		SpringApplication.run(BiApplication.class, args);
		BiController articleservice = new BiController();
		articleservice.articlesCountry();
	}

}
