include_directories(../include)
include_directories(../include/catch)

# ï¿½ï¿½ï¿½Ó²ï¿½ï¿½Ô¿ï¿½Ö´ï¿½ï¿½ï¿½Ä¼ï¿½
add_executable(ap_tests
    test_ap.cpp
)

# ï¿½ï¿½ï¿½ï¿½Â·ï¿½ï¿½ï¿½ï¿½Òªï¿½Ü·ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Ý¿ï¿½ï¿½Í·ï¿½Ä¼ï¿?
target_include_directories(ap_tests PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/bind
    ${PROJECT_SOURCE_DIR}/include/catch
)

<<<<<<< HEAD
# Á´½ÓÄãÒÑ¾­Éú³ÉµÄÊý¾Ý¿â
=======
# ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½Ñ¾ï¿½ï¿½ï¿½ï¿½Éµï¿½ï¿½ï¿½ï¿½Ý¿ï¿½ï¿?
>>>>>>> 8b359ba127986872042f36f6e4aec180fc0912f6
target_link_libraries(ap_tests PRIVATE bach-opd folly)

