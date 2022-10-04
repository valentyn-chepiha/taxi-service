package taxi.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import taxi.lib.Dao;
import taxi.model.Car;
import taxi.model.Driver;
import taxi.model.Manufacturer;
import taxi.util.ConnectionUtil;

@Dao
public class CarDaoImpl implements CarDao {
    private static final int ZERO_PLACEHOLDER = 0;
    private static final int SHIFT = 2;
    private static final Logger logger = LogManager.getLogger(CarDaoImpl.class);

    @Override
    public Car create(Car car) {
        logger.debug("Method create start.");
        String query = "INSERT INTO cars (model, manufacturer_id)"
                + "VALUES (?, ?)";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(
                             query, Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, car.getModel());
            statement.setLong(2, car.getManufacturer().getId());
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                car.setId(resultSet.getObject(1, Long.class));
            }
            logger.debug("Create car: " + car);
        } catch (SQLException e) {
            logger.error("Can't create car: " + car, e);
        }
        insertAllDrivers(car);
        return car;
    }

    @Override
    public Optional<Car> get(Long id) {
        logger.debug("Method get start.");
        String query = "SELECT c.id AS id, "
                + "model, "
                + "manufacturer_id, "
                + "m.name AS manufacturer_name, "
                + "m.country AS manufacturer_country "
                + "FROM cars c "
                + "JOIN manufacturers m ON c.manufacturer_id = m.id "
                + "WHERE c.id = ? AND c.is_deleted = FALSE";
        Car car = null;
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                car = parseCarFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            logger.error("Can't get car by id: " + id, e);
        }
        if (car != null) {
            car.setDrivers(getAllDriversByCarId(car.getId()));
        }
        logger.debug("Get car: " + car + " by id: " + id);
        return Optional.ofNullable(car);
    }

    @Override
    public List<Car> getAll() {
        logger.debug("Method getФll start.");
        String query = "SELECT c.id AS id, "
                + "model, "
                + "manufacturer_id, "
                + "m.name AS manufacturer_name, "
                + "m.country AS manufacturer_country "
                + "FROM cars c"
                + " JOIN manufacturers m ON c.manufacturer_id = m.id"
                + " WHERE c.is_deleted = FALSE";
        List<Car> cars = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                cars.add(parseCarFromResultSet(resultSet));
            }
            cars.forEach(car -> car.setDrivers(getAllDriversByCarId(car.getId())));
            logger.debug("Get list cars:");
            cars.forEach(logger::debug);
        } catch (SQLException e) {
            logger.error("Can't get all cars", e);
        }
        return cars;
    }

    @Override
    public Car update(Car car) {
        logger.debug("Method update start.");
        String query = "UPDATE cars SET model = ?, manufacturer_id = ? WHERE id = ?"
                + " AND is_deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            statement.setString(1, car.getModel());
            statement.setLong(2, car.getManufacturer().getId());
            statement.setLong(3, car.getId());
            statement.executeUpdate();
            deleteAllDriversExceptList(car);
            insertAllDrivers(car);
            logger.debug("Update car: " + car);
        } catch (SQLException e) {
            logger.error("Can't update car: " + car, e);
        }
        return car;
    }

    @Override
    public boolean delete(Long id) {
        logger.debug("Method delete start.");
        String query = "UPDATE cars SET is_deleted = TRUE WHERE id = ?"
                + " AND is_deleted = FALSE";
        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement statement =
                         connection.prepareStatement(query)) {
            statement.setLong(1, id);
            boolean resultDeleted = statement.executeUpdate() > 0;
            logger.debug("deleted by id: " + id + " is " + resultDeleted);
            return resultDeleted;
        } catch (SQLException e) {
            logger.error("Can't delete car by id: " + id, e);
            return false;
        }
    }

    @Override
    public List<Car> getAllByDriver(Long driverId) {
        logger.debug("Method getAllByDriver start.");
        String query = "SELECT c.id AS id, "
                + "model, "
                + "manufacturer_id, "
                + "m.name AS manufacturer_name, "
                + "m.country AS manufacturer_country "
                + "FROM cars c"
                + " JOIN manufacturers m ON c.manufacturer_id = m.id"
                + " JOIN cars_drivers cd ON c.id = cd.car_id"
                + " JOIN drivers d ON cd.driver_id = d.id"
                + " WHERE c.is_deleted = FALSE AND driver_id = ?"
                + " AND d.is_deleted = FALSE";
        List<Car> cars = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            statement.setLong(1, driverId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                cars.add(parseCarFromResultSet(resultSet));
            }
        } catch (SQLException e) {
            logger.error("Can't get all cars by driver id: " + driverId, e);
        }
        cars.forEach(car -> car.setDrivers(getAllDriversByCarId(car.getId())));
        logger.debug("Get list cars by driver id:");
        cars.forEach(logger::debug);
        return cars;
    }

    private void insertAllDrivers(Car car) {
        logger.debug("Method insertAllDrivers start.");
        Long carId = car.getId();
        List<Driver> drivers = car.getDrivers();
        if (drivers.size() == 0) {
            return;
        }
        String query = "INSERT INTO cars_drivers (car_id, driver_id) VALUES "
                + drivers.stream().map(driver -> "(?, ?)").collect(Collectors.joining(", "))
                + " ON DUPLICATE KEY UPDATE car_id = car_id";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            for (int i = 0; i < drivers.size(); i++) {
                Driver driver = drivers.get(i);
                statement.setLong((i * SHIFT) + 1, carId);
                statement.setLong((i * SHIFT) + 2, driver.getId());
            }
            statement.executeUpdate();
            logger.debug("All drivers insert to car:" + car);
        } catch (SQLException e) {
            logger.error("Can't insert drivers " + drivers, e);
        }
    }

    private void deleteAllDriversExceptList(Car car) {
        logger.debug("Method deleteAllDriversExceptList start.");
        Long carId = car.getId();
        List<Driver> exceptions = car.getDrivers();
        int size = exceptions.size();
        String query = "DELETE FROM cars_drivers WHERE car_id = ? "
                + "AND NOT driver_id IN ("
                + ZERO_PLACEHOLDER + ", ?".repeat(size)
                + ");";
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            statement.setLong(1, carId);
            for (int i = 0; i < size; i++) {
                Driver driver = exceptions.get(i);
                statement.setLong((i) + SHIFT, driver.getId());
            }
            statement.executeUpdate();
            logger.debug("All drivers delete from car:" + car);
        } catch (SQLException e) {
            logger.error("Can't delete drivers " + exceptions, e);
        }
    }

    private List<Driver> getAllDriversByCarId(Long carId) {
        logger.debug("Method getAllDriversByCarId start.");
        String query = "SELECT id, name, license_number, login, password "
                + "FROM cars_drivers cd "
                + "JOIN drivers d ON cd.driver_id = d.id "
                + "WHERE car_id = ? AND is_deleted = false";
        List<Driver> drivers = new ArrayList<>();
        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement statement =
                        connection.prepareStatement(query)) {
            statement.setLong(1, carId);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                drivers.add(parseDriverFromResultSet(resultSet));
            }
            logger.debug("Get drivers by car id:");
            drivers.forEach(logger::debug);
        } catch (SQLException e) {
            logger.error("Can't get all drivers by car id " + carId, e);
        }
        return drivers;
    }

    private Driver parseDriverFromResultSet(ResultSet resultSet) throws SQLException {
        Long driverId = resultSet.getObject("id", Long.class);
        String name = resultSet.getNString("name");
        String licenseNumber = resultSet.getNString("license_number");
        String login = resultSet.getNString("login");
        String password = resultSet.getNString("password");

        Driver driver = new Driver();
        driver.setId(driverId);
        driver.setName(name);
        driver.setLogin(login);
        driver.setPassword(password);
        driver.setLicenseNumber(licenseNumber);
        return driver;
    }

    private Car parseCarFromResultSet(ResultSet resultSet) throws SQLException {
        Long manufacturerId = resultSet.getObject("manufacturer_id", Long.class);
        String manufacturerName = resultSet.getNString("manufacturer_name");
        String manufacturerCountry = resultSet.getNString("manufacturer_country");
        Manufacturer manufacturer = new Manufacturer();
        manufacturer.setId(manufacturerId);
        manufacturer.setName(manufacturerName);
        manufacturer.setCountry(manufacturerCountry);
        Long carId = resultSet.getObject("id", Long.class);
        String model = resultSet.getNString("model");
        Car car = new Car();
        car.setId(carId);
        car.setModel(model);
        car.setManufacturer(manufacturer);
        return car;
    }
}
