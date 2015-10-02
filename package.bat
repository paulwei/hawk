call mvn clean:clean
call mvn eclipse:clean
call mvn eclipse:eclipse
call mvn -Dmaven.test.skip=true package -U
@pause