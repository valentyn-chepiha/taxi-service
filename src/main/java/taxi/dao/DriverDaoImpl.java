package taxi.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import taxi.lib.Dao;
import taxi.model.Driver;
import taxi.util.ConnectionUtil;

@Dao
public class DriverDaoImpl implements DriverDao {
    private static final Logger logger = LogManager.getLogger(DriverDaoImpl.class);

    @Override
    public Driver create(Driver driver) {
        logger.debug("Method create start.");
        String query = "INSERT INTO drivers (name, license_number, login, password) "
                + "VALUES (?, ?, ?, ?)";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement = connection.prepareStatement(query,
                        Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, driver.getName());
            statement.setString(2, driver.getLicenseNumber());
            statement.setString(3, driver.getLogin());
            statement.setString(4, driver.getPassword());
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                driver.setId(resultSet.getObject(1, Long.class));
            }
            logger.debug("Create driver: " + driver);
        } catch (SQLException e) {
            logger.error("Couldn't create driver: " + driver + ". ", e);
        }
        return driver;
    }

    @Override
    public Optional<Driver> get(Long id) {
        logger.debug("Method get start.");
        String query = "SELECT * FROM drivers WHERE id = ? AND is_deleted = FALSE";
        Driver driver = null;
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                driver = parseDriverFromResultSet(resultSet);
            }
            logger.debug("Get driver: " + driver + " by id: " + id);
        } catch (SQLException e) {
            logger.debug("Couldn't get driver by id " + id, e);
        }
        return Optional.ofNullable(driver);
    }

    @Override
    public List<Driver> getAll() {
        logger.debug("Method getФll start.");
        String query = "SELECT * FROM drivers WHERE is_deleted = FALSE";
        List<Driver> drivers = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                drivers.add(parseDriverFromResultSet(resultSet));
            }
            logger.debug("Get list drivers:");
            drivers.forEach(logger::debug);
        } catch (SQLException e) {
            logger.error("Couldn't get a list of drivers from driversDB.", e);
        }
        return drivers;
    }

    @Override
    public Driver update(Driver driver) {
        logger.debug("Method update start.");
        String query = "UPDATE drivers "
                + "SET name = ?, license_number = ?, login = ?, password = ? "
                + "WHERE id = ? AND is_deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement
                        = connection.prepareStatement(query)) {
            statement.setString(1, driver.getName());
            statement.setString(2, driver.getLicenseNumber());
            statement.setString(3, driver.getLogin());
            statement.setString(4, driver.getPassword());
            statement.setLong(5, driver.getId());
            statement.executeUpdate();
            logger.debug("Update driver: " + driver);
        } catch (SQLException e) {
            logger.error("Couldn't update " + driver + " in driversDB.", e);
        }
        return driver;
    }

    @Override
    public boolean delete(Long id) {
        logger.debug("Method delete start.");
        String query = "UPDATE drivers SET is_deleted = TRUE WHERE id = ?";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setLong(1, id);
            boolean resultDeleted = statement.executeUpdate() > 0;
            logger.debug("deleted by id: " + id + " is " + resultDeleted);
            return resultDeleted;
        } catch (SQLException e) {
            logger.error("Couldn't delete driver with id " + id, e);
            return false;
        }
    }

    private Driver parseDriverFromResultSet(ResultSet resultSet) throws SQLException {
        Long id = resultSet.getObject("id", Long.class);
        String login = resultSet.getNString("login");
        String password = resultSet.getNString("password");
        String name = resultSet.getString("name");
        String licenseNumber = resultSet.getString("license_number");

        Driver driver = new Driver();
        driver.setId(id);
        driver.setLogin(login);
        driver.setPassword(password);
        driver.setName(name);
        driver.setLicenseNumber(licenseNumber);
        return driver;
    }

    @Override
    public Optional<Driver> findByLogin(String login) {
        logger.debug("Method findByLogin start.");
        String query = "SELECT * FROM drivers WHERE login = ? AND is_deleted = FALSE";
        Driver driver = null;
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, login);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                driver = parseDriverFromResultSet(resultSet);
            }
            logger.debug("Get driver: " + driver + " by login: " + login);
        } catch (SQLException e) {
            logger.error("Couldn't get driver by login = " + login, e);
        }
        return Optional.ofNullable(driver);
    }
}
