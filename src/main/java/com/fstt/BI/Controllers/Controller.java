package com.fstt.BI.Controllers;


import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fstt.BI.Service.ArticleService;
import com.fstt.BI.model.Article;



@RestController
public class Controller {

	
	@GetMapping("/api/articles/")
	public List<Article> allArticles() {
	ArticleService articleservice = new ArticleService();
	return articleservice.getAllArticles();
	}
	
	
	
}
