from bson.objectid import ObjectId
from pymongo import MongoClient, errors

# from config.deployConfig import DeployConfig
# from framework.formats.err import Error
# from framework.formats.model import IModel
# from framework.frontController.security.user import User
# from framework.frontController.sortedRequest.sr import ISr
# from sharedUtils.dateTimeEnhanced import DateTimeEnhanced as dte


class MongodbModel():

    def __init__(self,client,db):
        self.client = client
        self.db = db

    def __convertObjectIdsToStr__(self, data):
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, ObjectId):
                    data[key] = str(value)
                elif isinstance(value, dict) or isinstance(value, list):
                    self.__convertObjectIdsToStr__(value)
        elif isinstance(data, list):
            for index, item in enumerate(data):
                if isinstance(item, ObjectId):
                    data[index] = str(item)
                elif isinstance(item, dict) or isinstance(item, list):
                    self.__convertObjectIdsToStr__(item)
        return data

    # def __mongodbLogin__(self):
    #     try:
    #         self.client = MongoClient(
    #             f"mongodb://{self.mongoUser}:{self.mongoPassword}@{self.mongoHost}:{self.mongoPort}/{self.mongoDatabase}"
    #         )
    #         self.db = self.client[self.mongoDatabase]
    #     except:
    #         try:
    #             self.client = MongoClient(
    #                 f"mongodb://{self.mongoUser}:{self.mongoPassword}@{self.mongoHost}:{self.mongoPort}/{self.mongoDatabase}"
    #             )
    #             self.db = self.client[self.mongoDatabase]
    #         except:
    #             self.client = None
    #             self.db = None

    def __createMongodb__(self, collectionName, data):
        # self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                inserted_id = collection.insert_one(data).inserted_id
                self.client.close()
                inserted_id = str(inserted_id)
                return inserted_id
            except Exception as e:
                self.client.close()
                if "duplicate" in str(e):
                    self.setError(2406, f"Requested entity already exists")
                    return None
                else:
                    self.setError(1212, "Mongodb execution failed: " + str(e))
                    return None
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    def __deleteMongodb__(self, collectionName, identifierName, identifierValue):
        # self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                if identifierName == "_id":
                    identifierValue = ObjectId(identifierValue)
                filter_query = {identifierName: identifierValue}
                deletedCount = collection.delete_one(filter_query).deleted_count
                self.client.close()
                return deletedCount
            except Exception as e:
                self.client.close()
                self.setError(1212, "Mongodb execution failed: " + str(e))
                return None
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    def __getMongodb__(
        self, collectionName, isActive=False, identifierName=None, identifierValue=None
    ):
        # self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                filterQuery = dict()
                if identifierName and isinstance(identifierValue, list):
                    filterQuery = {identifierName: {"$in": identifierValue}}
                elif (
                    (identifierName is None) and (identifierValue is None) and isActive
                ):
                    filterQuery = {"isActive": True}
                elif (
                    (identifierName is not None)
                    and (identifierValue is not None)
                    and not isActive
                ):
                    if identifierName == "_id":
                        identifierValue = ObjectId(identifierValue)
                    filterQuery = {identifierName: identifierValue}
                elif (
                    (identifierName is not None)
                    and (identifierValue is not None)
                    and isActive
                ):
                    if identifierName == "_id":
                        identifierValue = ObjectId(identifierValue)
                    filterQuery = {identifierName: identifierValue, "isActive": True}

                if not filterQuery:
                    documents = collection.find()
                else:
                    documents = collection.find(filterQuery)

                if documents:
                    result = list(documents)
                else:
                    result = []
                result = self.__convertObjectIdsToStr__(result)
                return result
            except Exception as e:
                self.setError(1212, f"An error occurred while fetching documents: {e}")
                return None
            finally:
                self.client.close()
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    # commit procedures
    def createNode(self, data):
        inserted_id = self.__createMongodb__("Node", data)
        self.output = inserted_id

    def createTheme(self, data):
        inserted_id = self.__createMongodb__("Theme", data)
        self.output = inserted_id

    def createInstitute(self, data):
        inserted_id = self.__createMongodb__("Institute", data)
        self.output = inserted_id

    def createAccreditor(self, data):
        inserted_id = self.__createMongodb__("Accreditor", data)
        self.output = inserted_id

    def createAccreditationType(self, data):
        inserted_id = self.__createMongodb__("AccreditationType", data)
        self.output = inserted_id

    def createOwner(self, data):
        inserted_id = self.__createMongodb__("Owner", data)
        self.output = inserted_id

    def createClassCategory(self, data):
        inserted_id = self.__createMongodb__("ClassCategory", data)
        self.output = inserted_id

    def createCourse(self, data):
        inserted_id = self.__createMongodb__("Course", data)
        self.output = inserted_id

    def createSpecialization(self, data):
        inserted_id = self.__createMongodb__("Specialization", data)
        self.output = inserted_id

    def createStream(self, data):
        inserted_id = self.__createMongodb__("Stream", data)
        self.output = inserted_id

    def createUserQualification(self, data):
        inserted_id = self.__createMongodb__("UserQualification", data)
        self.output = inserted_id

    def createQualification(self, data):
        inserted_id = self.__createMongodb__("Qualification", data)
        self.output = inserted_id

    def createEducation(self, data):
        # formattedData = {
        #     'instituteUUID': data['instituteUUID'],
        #     'courseUUID': data['courseUUID'],
        #     'class': data['class'],
        #     'startYear': data['startYear'],
        #     'endYear': data['endYear'],
        #     'accreditionUUI': data['accreditionUUID']
        # }
        inserted_id = self.__createMongodb__("Education", data)
        self.output = inserted_id

    def createCertificate(self, data):
        inserted_id = self.__createMongodb__("Certificate", data)
        self.output = inserted_id

    def createCertificateThemeTemplate(self, data):
        inserted_id = self.__createMongodb__("CertificateThemeTemplate", data)
        self.output = inserted_id

    def createCertificateDataTemplate(self, data):
        inserted_id = self.__createMongodb__("CertificateDataTemplate", data)
        self.output = inserted_id

    def createIssuedCertificate(self, data):
        inserted_id = self.__createMongodb__("IssuedCertificate", data)
        self.output = inserted_id

    def createUser(self, data):
        inserted_id = self.__createMongodb__("User", data)
        self.output = inserted_id

    def createParent(self, data):
        inserted_id = self.__createMongodb__("Parent", data)
        self.output = inserted_id

    def createProgram(self, data):
        formattedData = {
            "programUUID": data["programUUID"],
            "tags": data["tags"],
            "programDescription": data["programDescription"],
            "introVideo": data["introVideo"],
            "thumbnail": data["thumbnail"],
            "isGradedCertificateAvailable": data["isGradedCertificateAvailable"],
            "gradedCertificateTemplate": (
                data["gradedCertificateTemplate"]
                if "gradedCertificateTemplate" in data
                else None
            ),
            "isCompletionCertificateAvailable": data[
                "isCompletionCertificateAvailable"
            ],
            "completionCertificateTemplate": (
                data["completionCertificateTemplate"]
                if "completionCertificateTemplate" in data
                else None
            ),
            "partners": data["partners"] if "partners" in data else [],
            "slide": data["slide"],
        }
        inserted_id = self.__createMongodb__("Program", formattedData)
        self.output = inserted_id

    def createEdition(self, data):
        formattedData = {
            "programEditionUUID": data["programEditionUUID"],
            "about": data["about"],
            "introVideo": data["introVideo"],
            "thumbnail": data["thumbnail"],
            "isGradedCertificateAvailable": data["isGradedCertificateAvailable"],
            "gradedCertificateTemplate": (
                data["gradedCertificateTemplate"]
                if "gradedCertificateTemplate" in data
                else None
            ),
            "isCompletionCertificateAvailable": data[
                "isCompletionCertificateAvailable"
            ],
            "completionCertificateTemplate": (
                data["completionCertificateTemplate"]
                if "completionCertificateTemplate" in data
                else None
            ),
            "partners": data["partners"] if "partners" in data else [],
            "slide": data["slide"],
        }
        if "eligibility" in data:
            formattedData["eligibility"] = data["eligibility"]
        inserted_id = self.__createMongodb__("ProgramEdition", formattedData)
        self.output = inserted_id

    def createSlot(self, data):
        formattedData = {
            "programEditionSlotUUID": data["programEditionSlotUUID"],
            "isGraded": data["isGraded"],
            "partners": data["partners"] if "partners" in data else [],
            "slide": data["slide"] if "slide" in data else None,
        }
        if "gradeSource" in data:
            formattedData["gradeSource"] = data["gradeSource"]
        if "isCompletionCertificateAvailable" in data:
            formattedData["isCompletionCertificateAvailable"] = data[
                "isCompletionCertificateAvailable"
            ]
        if "isGradedCertificateAvailable" in data:
            formattedData["isGradedCertificateAvailable"] = data[
                "isGradedCertificateAvailable"
            ]
        if "completionCertificateId" in data:
            formattedData["completionCertificateId"] = data["completionCertificateId"]
        if "gradedCertificateId" in data:
            formattedData["gradedCertificateId"] = data["gradedCertificateId"]
        if "restrictions" in data:
            formattedData["restrictions"] = data["restrictions"]

        inserted_id = self.__createMongodb__("ProgramEditionSlot", formattedData)
        self.output = inserted_id

    def createTopic(self, data):
        inserted_id = self.__createMongodb__("Topic", data)
        self.output = inserted_id

    def createMessageQueue(self, data):
        inserted_id = self.__createMongodb__("MessageQueue", data)
        self.output = inserted_id

    def createNotification(self, data):
        inserted_id = self.__createMongodb__("Notification", data)
        self.output = inserted_id

    def createUserProfileView(self, data):
        formattedData = {
            "userUUID": data["userUUID"],
            "userName": data["userName"],
            "firstName": data["firstName"],
            "lastName": data["lastName"],
            "displayName": data["displayName"],
            "email": data["email"],
            "phoneNumber": data["phoneNumber"],
            "phoneNumberForWA": data["phoneNumberForWA"],
            "lmsUsername": data["lmsUsername"],
            "gender": data["gender"],
            "dateOfBirth": data["dateOfBirth"],
            "city": data["city"],
            "state": data["state"],
            "zipcode": data["zipcode"],
            "userType": data["userType"],
            "profilePicture": data["profilePicture"],
            "registrationMethod": data["registrationMethod"],
            "isEmailVerified": data["isEmailVerified"],
            "isPhoneVerified": data["isPhoneVerified"],
            "isProfileComplete": data["isProfileComplete"],
            "isActive": data["isActive"],
            "whatsappNotificationConsent": data["whatsappNotificationConsent"],
        }
        inserted_id = self.__createMongodb__("UserProfileView", formattedData)
        self.output = inserted_id

    def updateValue(self, data):
        collectionName, keyName, keyValue, identifierName, identifierValue = (
            data["collectionName"],
            data["keyName"],
            data["keyValue"],
            data["identifierName"],
            data["identifierValue"],
        )
        self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                if identifierName == "_id":
                    identifierValue = ObjectId(identifierValue)
                filter_query = {identifierName: identifierValue}
                update_operation = {"$set": {keyName: keyValue}}
                result = collection.update_one(filter_query, update_operation)
                if result.matched_count > 0:
                    if result.modified_count > 0:
                        self.client.close()
                        self.output = []
                    else:
                        # self.setError(2409, f"Requested entity already of collection {keyName} up-to-date")
                        self.client.close()
                        self.output = []
                        print(
                            f"Requested entity already of collection {keyName} up-to-date"
                        )
                        return None
                else:
                    self.setError(
                        2409,
                        f"Requested entity does not exist."
                        + f"{identifierName} + {identifierValue}",
                    )
                    return None
            except errors.PyMongoError as e:
                self.setError(
                    1212, f"An error occurred while updating the document: {e}"
                )
                return None
            finally:
                self.client.close()
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    def pushValue(self, data):
        collectionName, keyName, keyValue, identifierName, identifierValue = (
            data["collectionName"],
            data["keyName"],
            data["keyValue"],
            data["identifierName"],
            data["identifierValue"],
        )
        self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                if identifierName == "_id":
                    identifierValue = ObjectId(identifierValue)
                filter_query = {identifierName: identifierValue}
                update_operation = {"$addToSet": {keyName: keyValue}}
                result = collection.update_one(filter_query, update_operation)
                if result.matched_count > 0:
                    if result.modified_count > 0:
                        self.client.close()
                        self.output = []
                    else:
                        # self.setError(2409, f"Requested entity already of collection {keyName} up-to-date")
                        self.client.close()
                        self.output = []
                        print(
                            f"Requested entity already of collection {keyName} up-to-date"
                        )
                        return None
                else:
                    self.setError(2409, f"Requested entity does not exist.")
                    return None
            except errors.PyMongoError as e:
                self.setError(
                    1212, f"An error occurred while updating the document: {e}"
                )
                return None
            finally:
                self.client.close()
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    def pullValue(self, data):
        collectionName, keyName, keyValue, identifierName, identifierValue = (
            data["collectionName"],
            data["keyName"],
            data["keyValue"],
            data["identifierName"],
            data["identifierValue"],
        )
        self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db[collectionName]
                if identifierName == "_id":
                    identifierValue = ObjectId(identifierValue)
                filter_query = {identifierName: identifierValue}
                update_operation = {"$pull": {keyName: keyValue}}
                result = collection.update_one(filter_query, update_operation)
                if result.matched_count > 0:
                    if result.modified_count > 0:
                        self.client.close()
                        self.output = []
                    else:
                        # self.setError(2409, f"Requested entity already of collection {keyName} up-to-date")
                        self.client.close()
                        self.output = []
                        print(
                            f"Requested entity already of collection {keyName} up-to-date"
                        )
                        return None
                else:
                    self.setError(2409, f"Requested entity does not exist.")
                    return None
            except errors.PyMongoError as e:
                self.setError(
                    1212, f"An error occurred while updating the document: {e}"
                )
                return None
            finally:
                self.client.close()
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    def deleteValue(self, data):
        deletedCount = self.__deleteMongodb__(
            data["collectionName"], data["identifierName"], data["identifierValue"]
        )
        if deletedCount:
            self.output = deletedCount
        else:
            self.setError(1212, "Execution Failed")
            return None

    # get / fetch procedures
    def checkEntityExists(self, data):
        result = self.__getMongodb__(
            data["collectionName"],
            False,
            data["identifierName"],
            data["identifierValue"],
        )
        if isinstance(result, list):
            self.output = result
        else:
            self.output = None

    def getAllData(self, data):
        result = self.__getMongodb__(data["collectionName"])
        if isinstance(result, list):
            self.output = result
        else:
            self.output = None

    def getAllActiveData(self, data):
        result = self.__getMongodb__(data["collectionName"], True)
        if isinstance(result, list):
            return result
        else:
            return None

    def getDataById(self, data):
        result = self.__getMongodb__(
            data["collectionName"],
            False,
            data["identifierName"],
            data["identifierValue"],
        )
        if isinstance(result, list):
            self.output = result
        else:
            self.output = None

    def getActiveDataById(self, data):
        result = self.__getMongodb__(
            data["collectionName"],
            True,
            data["identifierName"],
            data["identifierValue"],
        )
        if isinstance(result, list):
            self.output = result
        else:
            self.output = None

    def getUserDetails(self, data):
        result = self.__getMongodb__("User", False, "userUUID", data["userUUID"])
        if isinstance(result, list):
            self.output = result
        else:
            self.output = None

    def deleteMessageQueue(self):
        self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db["MessageQueue"]
                filter_query = {
                    "processedBy": {
                        "$exists": True,  # Ensure processedBy exists
                        "$type": "object",  # Ensure it's an object
                    },
                    "$expr": {
                        "$eq": [
                            {
                                "$size": {"$objectToArray": "$processedBy"}
                            },  # Number of keys in processedBy
                            {
                                "$size": {
                                    "$filter": {  # Number of keys with value True
                                        "input": {"$objectToArray": "$processedBy"},
                                        "cond": {"$eq": ["$$this.v", True]},
                                    }
                                }
                            },
                        ]
                    },
                }
                deletedCount = collection.delete_many(filter_query).deleted_count
                self.client.close()
                self.output = deletedCount
            except Exception as e:
                self.client.close()
                self.setError(1212, "Mongodb execution failed: " + str(e))
                return None
        else:
            self.setError(1212, "Unable to reach mongodb server")
            return None

    def deleteNotification(self):
        self.__mongodbLogin__()
        if (self.client is not None) and (self.db is not None):
            try:
                collection = self.db["Notification"]
                current_time = dte.utcnow()  # Get current UTC time
                # Query to match documents with expiryTime < current time
                filter_query = {"expiryTime": {"$lt": current_time}}

                # Delete matching documents
                deletedCount = collection.delete_many(filter_query).deleted_count
                self.client.close()
                self.output = deletedCount
            except Exception as e:
                self.client.close()
                self.setError(1213, "Mongodb execution failed: " + str(e))
                return None
        else:
            self.setError(1213, "Unable to reach mongodb server")
            return None
