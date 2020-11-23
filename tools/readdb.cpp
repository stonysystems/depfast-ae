#include "cstdio"
#include "iostream"
using namespace std;
typedef int32_t i32;

struct KeyValue {
	int key;
	i32 value;
};


int main(){
	std::string file="/db/data.txt";
	size_t read_;
	size_t written_;

	long lsize;

	FILE* f = fopen(file.c_str(), "rb");
    fseek(f, 0, SEEK_END);
    lsize=ftell(f);
    rewind(f);

	size_t size_=sizeof(struct KeyValue);
	size_t count_=lsize/size_;
	struct KeyValue key_value_[count_];

	if (f != NULL) {
		read_ = fread(key_value_, size_, count_, f);
		fclose(f);
	}
    
    int cnt=0;
    for(int i=0;i<count_;i++){
	    std::cout<<key_value_[i].key<<"  "<<key_value_[i].value<<std::endl;
	    if(key_value_[i].value!=-1)
	    	cnt++;
	}
    cout<<lsize<<" --- "<<size_<<" -- "<<cnt<<endl;

}