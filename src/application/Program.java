package application;



import java.sql.Date;
import java.util.List;
import java.util.Scanner;

import model.dao.DaoFactory;
import model.dao.DepartmentDao;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class Program {

	public static void main(String[] args) {
		
		
//		========================= PROGRAM 1 -Testing Seller CRUD ===============================
		
		
//		Scanner sc = new Scanner(System.in);
//		
//		SellerDao sellerDao = DaoFactory.createSellerDao();
//		
//		System.out.println("=== TEST 1: seller findById =====");
//		Seller seller = sellerDao.findById(3);
//		
//		System.out.println(seller);
//		System.out.println("=== TEST 2: seller findByDepartment =====");
//		
//		Department department = new Department(2, null);
//		List<Seller> list = sellerDao.findByDepartment(department);
//		for (Seller obj : list) {
//			System.out.println(obj);
//		}
//		
//		System.out.println("=== TEST 3: seller findAll =====");
//		list = sellerDao.findAll();
//		for (Seller obj : list) {
//			System.out.println(obj);
//		}
//		
//		System.out.println("=== TEST 4: seller insert =====");
//		Seller newSeller = new Seller(null, "Greg", "greg@gmail.com", new java.util.Date(), 4000.0, department);
//		sellerDao.insert(newSeller);
//		System.out.println("Inserted! New Id = " + newSeller.getId());
//		
//		
//		System.out.println("=== TEST 5: seller update =====");
//		seller = sellerDao.findById(1);
//		seller.setName("Marta Waine");
//		sellerDao.update(seller);
//		System.out.println("Update completed");
//		
//		System.out.println("=== TEST 6: seller delete =====");
//		int id = sc.nextInt();
//		sellerDao.deleteById(id);
//		System.out.println("Deleted element");
		
		
//		======================== PROGRAM 2 - Testing Department CRUD ============================
		
		
//		Scanner sc = new Scanner(System.in);
//		
//		DepartmentDao departmentDao = DaoFactory.createDepartmentDao();

//		System.out.println("=== TEST 1: findById =======");
//		Department department = departmentDao.findById(5);
//		System.out.println(department);
		
//		System.out.println("\n=== TEST 2: findAll =======");
//		List<Department> list = departmentDao.findAll();
//		for (Department d : list) {
//			System.out.println(d);
//		}
		
//		System.out.println("\n=== TEST 3: insert =======");
//		Department newDepartment = new Department(null, "Music");
//		departmentDao.insert(newDepartment);
//		System.out.println("Inserted! New id: " + newDepartment.getId());
		
//		System.out.println("\n=== TEST 4: update =======");
//		Department dep2 = departmentDao.findById(6);
//		dep2.setName("Food");
//		departmentDao.update(dep2);
//		System.out.println("Update completed");
		
//		System.out.println("\n=== TEST 5: delete =======");
//		System.out.print("Enter id for delete test: ");
//		int id = sc.nextInt();
//		departmentDao.deleteById(id);
//		System.out.println("Delete completed");
//		
//		sc.close();
		
	}

}
