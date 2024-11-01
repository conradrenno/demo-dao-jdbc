package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{

	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Department obj) {
		PreparedStatement statement = null;
		
		try {
			statement = conn.prepareStatement("INSERT INTO Department "
					+ "(Name) "
					+ "VALUES "
					+ "(?)",
					statement.RETURN_GENERATED_KEYS);
			
			statement.setString(1, obj.getName());
			
			int rowsAffected = statement.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if(resultSet.next()) {
					int id = resultSet.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(resultSet);
			} else {
				throw new DbException("Unexpected error! No rows affected!");
			}
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
		}
		
		
	}

	@Override
	public void update(Department obj) {
		PreparedStatement statement = null;
		
		try {
			statement = conn.prepareStatement(
					"UPDATE department "
				  + "SET Name = ? "
				  + "WHERE id = ?");
			
			statement.setString(1, obj.getName());
			statement.setInt(2,obj.getId());
			
			statement.executeUpdate();
			
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement statement = null;
		
		try {
			statement = conn.prepareStatement("DELETE FROM department WHERE Id = ?");
			
			statement.setInt(1, id);
			
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			
		}
		
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement statement= null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement("SELECT department.* FROM department WHERE department.Id = ?");
			statement.setInt(1, id);
			
			resultSet = statement.executeQuery();
			if(resultSet.next()) {
				Department department= instantiateDepartment(resultSet);
				return department;
			}
			return null;
		} catch (Exception e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
		
		
	}

	@Override
	public List<Department> findAll() {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = conn.prepareStatement("SELECT department.* FROM department ORDER BY Name" );
			resultSet = statement.executeQuery();
			List<Department> departments = new ArrayList<Department>();
			while(resultSet.next()) {
				Department department = instantiateDepartment(resultSet);
				departments.add(department);
			}
			return departments;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}
		
		
		
	}

	
	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department department = new Department();
		department.setId(rs.getInt("Id"));
		department.setName(rs.getString("Name"));
		return department;
	}
	
}
