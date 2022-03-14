package main

import (
	"fmt"
	"github.com/araddon/dateparse"
	uuid "github.com/nu7hatch/gouuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"strconv"

	"os"
	"strings"
	"time"
)

type Result struct {
	CID            int        `json:"cid,omitempty"`
	MakerId        int        `json:"maker_id,omitempty"`
	ClientId       int        `json:"client_id,omitempty"`
	WarehouseId    int        `json:"warehouse_id,omitempty"`
	CommodityId    int        `json:"commodity_id,omitempty"`
	ActiveStatus   int        `json:"active_status,omitempty"`
	ChrCreateddate *time.Time `json:"chr_created_date,omitempty"`
	Chamber        string     `json:"chamber,omitempty"`
	Stack          string     `json:"stack,omitempty"`
	Bag            []uint8    `json:"bag,omitempty"`
	Quantity       string     `json:"quantity,omitempty"`
	FieldName      string     `json:"field_name,omitempty"`
	FieldValue     string     `json:"field_value,omitempty"`
	FumigationDate string     `json:"fumigation_date,omitempty"`
	SprayDate      string     `json:"spray_date,omitempty"`
	CleaninessDate string     `json:"remark,omitempty"`
	CdCreateddate  *time.Time `json:"cd_created_date,omitempty"`
}

type DTRAudit struct {
	ID            int        `json:"id"`
	RequestId     string     `json:"request_id"`
	AuditID       int        `json:"audit_id"`
	MakerID       int        `json:"maker_id"`
	EnduserID     int        `json:"enduser_id"`
	FarmerID      int        `gorm:"-" json:"farmer_id"`
	WarehouseID   int        `json:"warehouse_id"`
	ClientId      int        `json:"client_id"`
	CommodityType string     `json:"commodity_type"`
	FormType      string     `json:"form_type"`
	DtrType       string     `json:"dtr_type"`
	ContractId    int        `json:"contract_id"`
	CommodityId   int        `json:"commodity_id"`
	StockCount    float64    `json:"stock_count"`
	BagsCount     []uint8    `json:"bags_count"`
	StkNo         string     `json:"stk_no"`
	BagWeight     float64    `json:"bag_weight"`
	Approved      string     `gorm:"default:0" json:"approved"`
	Source        string     `json:"source"`
	UpdatedSource string     `json:"updated_source"`
	UserAgent     string     `json:"user_agent"`
	CreatedAt     *time.Time `gorm:"<-:create" json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	ApprovedBy    int        `json:"approved_by"`
}

type DTRQuality struct {
	ID                                    int        `json:"id"`
	DtrAuditId                            int        `json:"dtr_audit_id"`
	Moisture                              string     `json:"moisture"`
	Shriveled                             float32    `json:"shriveled"`
	Broken                                float32    `json:"broken"`
	Discoloured                           float32    `json:"discoloured"`
	Damage                                float32    `json:"damage"`
	Weevilled                             float32    `json:"weevilled"`
	InsectsDamageKernelsOrWeevilledGrains float32    `json:"insects_damage_kernels_or_weevilled_grains"`
	GreenSeed                             float32    `json:"green_seed"`
	Infestation                           float32    `json:"infestation"`
	OtherFoodGrain                        float32    `json:"other_food_grain"`
	WaterDamage                           float32    `json:"water_damage"`
	Undevelop                             float32    `json:"undevelop"`
	BrokenShriveled                       float32    `json:"broken_shriveled_and_immature"`
	SplitAndBroken                        float32    `json:"split_and_broken"`
	Fungus                                float32    `json:"fungus"`
	ImmatureAndShriveled                  float32    `json:"immature_and_shriveled"`
	Insects_Damage_Kernels                float32    `json:"insects_damage_kernels"`
	Length                                float32    `json:"length"`
	Chalky                                float32    `json:"chalky"`
	DamagedAndDiscoloured                 float32    `json:"damaged_and_discoloured"`
	OilContent                            float32    `json:"oil_content"`
	DefectiveRhizomes                     float32    `json:"defective_rhizomes"`
	HectoliterWeight                      float32    `json:"hectoliter_weight"`
	BlackTip                              float32    `json:"black_tip"`
	KarnalBunt                            float32    `json:"karnal_bunt"`
	Potiya                                float32    `json:"potiya"`
	SmallMudBall                          float32    `json:"small_mud_ball"`
	Khapra                                int        `json:"khapra"`
	RedGrain                              float32    `json:"red_grain"`
	AcidSolubleAshMax                     float32    `json:"acid_soluble_ash_max"`
	ExtraneousMatter                      float32    `json:"extraneous_matter"`
	Type                                  string     `json:"type"`
	QualityAssessmentDate                 *time.Time `json:"quality_assessment_date,omitempty"`
	GdnNo                                 string     `json:"gdn_no"`
	StkNo                                 string     `json:"stk_no"`
	BagsCount                             int        `json:"bags_count"`
	StockCount                            float64    `json:"stock_count"`
	FumigationDate                        *time.Time `json:"fumigation_date"`
	SprayDate                             *time.Time `json:"spray_date"`
	CleanlinessDate                       *time.Time `json:"cleanliness_date"`
	Remarks                               string     `json:"remarks"`
	Source                                string     `json:"source"`
	UpdatedSource                         string     `json:"updated_source"`
	UserAgent                             string     `json:"user_agent"`
	CreatedAt                             time.Time  `gorm:"<-:create" json:"created_at"`
	UpdatedAt                             time.Time  `json:"updated_at"`
}

func (DTRAudit) TableName() string {
	return "dtr_audit2"
}

func (DTRQuality) TableName() string {
	return "dtr_quality"
}

func evaluateString(str string) (response *time.Time) {
	//re := regexp.MustCompile("(0?[1-9]|1[012])/(0?[1-9]|[12][0-9]|3[01])/((19|20)\\d\\d)")
	//mm/dd//yyyy
	if len(str) > 0 && str != "0" && str != "00" && str != "0000" && str != "000" {
		//parsedDate, _ := time.Parse("01/02/2006", str)
		t, _ := dateparse.ParseLocal(str)
		//fmt.Println(parsedDate)

		return &t
	} else {
		fmt.Println(str)
		return nil
	}
}

func main() {

	dsn := "root:1234@tcp(127.0.0.1:3306)/arya_dtr?charset=utf8mb4&parseTime=True&loc=Local"
	file, err := os.Create("gorm-log.txt")
	if err != nil {
		// Handle error
		panic(err)
	}
	// Make sure file is closed before your app shuts down.

	newLogger := logger.New(
		log.New(file, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second,   // Slow SQL threshold
			LogLevel:                  logger.Silent, // Log level
			IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,
		},
	)

	//db, err := gorm.Open("mysql", "root:1234@tcp(localhost:3306)/arya_dtr")
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: newLogger.LogMode(logger.Error),
	})

	//db.SingularTable(true)
	if err != nil {
		fmt.Printf("Error %v", err)
	}

	var result []Result
	if err := db.Raw("SELECT c.id AS cid, c.maker_id, c.client_id, c.warehouse_id, c.commodity_id, c.active_status, c.created_at AS chr_created_date, cd.chamber, cd.stack, cd.bag, cd.quantity, cd.field_name, cd.field_value, cd.fumigation_date, cd.spray_date, cd.cleaniness_date, cd.remark, cd.created_at AS cd_created_date FROM chr c, chr_detail cd WHERE c.id=cd.chr_id").Scan(&result).Error; err != nil {
		// return any error will rollback

	}
	//fmt.Println(result)

	for _, record := range result {

		db.Transaction(func(tx *gorm.DB) error {
			// do some database operations in the transaction (use 'tx' from this point, not 'db')

			// return nil will commit the whole transaction
			commodityType := "cotton"
			if strings.Contains(record.FieldName, "grain") {
				commodityType = "grain"

			}
			u, err := uuid.NewV4()
			dtr := DTRAudit{
				CreatedAt:     record.ChrCreateddate,
				MakerID:       record.MakerId,
				WarehouseID:   record.WarehouseId,
				ClientId:      record.ClientId,
				CommodityId:   record.CommodityId,
				StkNo:         record.Stack,
				BagsCount:     record.Bag,
				FormType:      "quality",
				DtrType:       "quality",
				CommodityType: commodityType,
				RequestId:     u.String(),
			}
			data := tx.Create(&dtr)
			//fmt.Println(dtr.ID)

			err = data.Error

			//Uploading to DTR Quality Table
			qualityMap := make(map[string]string)
			fieldNames := strings.Split(record.FieldName, ",")
			fieldValues := strings.Split(record.FieldValue, ",")

			for i, n := range fieldValues {
				qualityMap[fieldNames[i]] = n

			}

			// entry to dtr quality
			//[Moisture HLW Potiya Red Grain FM IDK Broken and Shrivilled KB BT SMB OFG Luster Loss and Discolour Live Insect]

			hlw, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["HLW"]), 32)
			potiya, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["Potiya"]), 32)
			rg, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["Red Grain"]), 32)
			idk, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["IDK"]), 32)
			bs, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["Broken and Shrivilled"]), 32)
			kb, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["KB"]), 32)
			smb, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["SMB"]), 32)
			ofg, err := strconv.ParseFloat(fmt.Sprintf("%.2s", qualityMap["OFG"]), 32)
			//fumigation, err := time.Parse("2006-01-02", record.FumigationDate.String)
			//SprayDate, err := time.Parse("2006-01-02", record.SprayDate.String)
			//CleaninessDate, err := time.Parse("2006-01-02", record.CleaninessDate.String)

			dtrQuality := DTRQuality{
				DtrAuditId:             dtr.ID,
				Moisture:               qualityMap["Moisture"],
				HectoliterWeight:       float32(hlw),
				Potiya:                 float32(potiya),
				RedGrain:               float32(rg),
				Insects_Damage_Kernels: float32(idk),
				BrokenShriveled:        float32(bs),
				KarnalBunt:             float32(kb),
				SmallMudBall:           float32(smb),
				OtherFoodGrain:         float32(ofg),
				QualityAssessmentDate:  record.CdCreateddate,
				FumigationDate:         evaluateString(record.FumigationDate),
				SprayDate:              evaluateString(record.SprayDate),
				CleanlinessDate:        evaluateString(record.CleaninessDate),
			}

			err = tx.Create(&dtrQuality).Error

			if err != nil {
				// return any error will rollback
				return err
			}

			return nil
		})

	}

}
