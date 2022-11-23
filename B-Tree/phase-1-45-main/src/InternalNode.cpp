#include "InternalNode.hpp"
#include "LeafNode.hpp"

//creates internal node pointed to by tree_ptr or creates a new one
InternalNode::InternalNode(const TreePtr &tree_ptr) : TreeNode(INTERNAL, tree_ptr) {
    this->keys.clear();
    this->tree_pointers.clear();
    if (!is_null(tree_ptr))
        this->load();
}

//max element from tree rooted at this node
Key InternalNode::max() {
    Key max_key = DELETE_MARKER;
    TreeNode* last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    max_key = last_tree_node->max();
    delete last_tree_node;
    return max_key;
}

RecordPtr InternalNode::max_rec() {
    // Key max_key = DELETE_MARKER;
    RecordPtr max_rec;
    TreeNode* last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    max_rec = last_tree_node->max_rec();
    delete last_tree_node;
    return max_rec;
}


Key InternalNode::min() {
    Key max_key = DELETE_MARKER;
    TreeNode* last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[0]);
    max_key = last_tree_node->min();
    delete last_tree_node;
    return max_key;
}

RecordPtr InternalNode::min_rec() {
    // Key max_key = DELETE_MARKER;
    RecordPtr max_rec;
    TreeNode* last_tree_node = TreeNode::tree_node_factory(this->tree_pointers[0]);
    max_rec = last_tree_node->min_rec();
    delete last_tree_node;
    return max_rec;
}


//if internal node contains a single child, it is returned
TreePtr InternalNode::single_child_ptr() {
    if (this->size == 1)
        return this->tree_pointers[0];
    return NULL_PTR;
}

//inserts <key, record_ptr> into subtree rooted at this node.
//returns pointer to split node if exists
//TODO: InternalNode::insert_key to be implemented
TreePtr InternalNode::insert_key(const Key &key, const RecordPtr &record_ptr) {
    TreePtr new_tree_ptr = NULL_PTR, curr;
    TreeNode* curr_node=NULL;
    int index;
    
    TreePtr potential_split_node_ptr=NULL_PTR;
    if(key > this->keys.back()){
        curr=this->tree_pointers.back();
        index=this->size-1;
    }
    else if(key <= this->keys[0]){
        curr = this->tree_pointers[0];
        index=0;
    }
    else{
        for(int i=1;i<this->keys.size();i++){
            if(key <= this->keys[i] && key > this->keys[i-1]){
                index=i;
                curr=this->tree_pointers[i];
                break;
            }
        }
    }
    curr_node = TreeNode::tree_node_factory(curr);
    potential_split_node_ptr = curr_node->insert_key(key,record_ptr);
    if(!is_null(potential_split_node_ptr)){
        Key key_new = curr_node->max();
        
        this->keys.insert(this->keys.begin()+index,key_new);
        this->tree_pointers.insert(this->tree_pointers.begin()+index+1,potential_split_node_ptr);
        this->size++;
        if(this->size > FANOUT){
            InternalNode* internal = new InternalNode();
            auto itr=this->keys.begin();
            for(int i=0;i<MIN_OCCUPANCY;i++)
                itr++;
            for(;itr!=this->keys.end();itr++)
                internal->keys.push_back(*itr);
            this->keys.erase(this->keys.begin()+MIN_OCCUPANCY-1,this->keys.end());


            auto itr2=this->tree_pointers.begin();
            for(int i=0;i<MIN_OCCUPANCY;i++)
                itr2++;

            for(;itr2!=this->tree_pointers.end();itr2++)
                internal->tree_pointers.push_back(*itr2);
            
            this->tree_pointers.erase(this->tree_pointers.begin()+MIN_OCCUPANCY,this->tree_pointers.end());

            internal->size=internal->tree_pointers.size();
            this->size-=internal->size;
            new_tree_ptr = internal->tree_ptr;
            internal->dump();
            delete internal;
        }
    }
    this->dump();
    return new_tree_ptr;
}

//deletes key from subtree rooted at this if exists
//TODO: InternalNode::delete_key to be implemented
void InternalNode::delete_key(const Key &key) {
    TreePtr new_tree_ptr = NULL_PTR,curr;
    // cout << "InternalNode::delete_key not implemented" << endl;
    TreeNode* curr_node=NULL;
    LeafNode* leaf=NULL;
    int index;
    
    if(key > this->keys.back()){
        curr=this->tree_pointers.back();
        index=this->size-1;
    }
    else if(key <= this->keys[0]){
        curr = this->tree_pointers[0];
        index=0;
    }
    else{
        for(int i=1;i<this->keys.size();i++){
            if(key <= this->keys[i] && key > this->keys[i-1]){
                index=i;
                curr=this->tree_pointers[i];
                break;
            }
        }
    }

    curr_node = TreeNode::tree_node_factory(curr);
    curr_node->delete_key(key);
    
    if(curr_node->underflows()){
        if(curr_node->node_type==LEAF){
            // left-sibling exists
            if(index>0){
                LeafNode* left=(LeafNode*)TreeNode::tree_node_factory(this->tree_pointers[index-1]);
                // LeafNode* left = (LeafNode*)left1;
                int size_of_left = left->size;
                // re-distribute
                if(size_of_left > MIN_OCCUPANCY){
                    Key key_max = left->max();
                    auto it = left->data_pointers.begin();
                    RecordPtr rec_max = it->second;
                    // RecordPtr rec_max = left->max_rec();
                    curr_node->insert_key(key_max,rec_max);
                    left->delete_key(key_max);
                    this->keys[index-1] = left->max();
                    curr_node->dump();
                }
                // merge
                else{
                    leaf=(LeafNode*)curr_node;
                    while(leaf->size > 0){
                        Key min_key = leaf->data_pointers.begin()->first;
                        RecordPtr min_rec = leaf->data_pointers.begin()->second;
                        leaf->delete_key(min_key);
                        left->insert_key(min_key,min_rec);
                    }
                    
                    left->next_leaf_ptr = leaf->next_leaf_ptr;
                    this->keys.erase(this->keys.begin() + (index - 1));
                    this->tree_pointers.erase(this->tree_pointers.begin() + index);
                    this->size--;
                    left->dump();
                }
                
                // curr_node->delete_node();
                delete left;
                // delete leaf;
            }
            // right-sibling 
            else if(index != this->tree_pointers.size()-1){
                LeafNode* right=(LeafNode*)TreeNode::tree_node_factory(this->tree_pointers[index+1]);
                // LeafNode* right = (LeafNode*)right1;
                leaf=(LeafNode*)curr_node;
                int size_of_right = right->size;
                // re-distribute
                if(size_of_right > MIN_OCCUPANCY){
                    Key key_min = right->data_pointers.begin()->first;
                    RecordPtr rec_min = right->data_pointers.begin()->second;
                    leaf->insert_key(key_min,rec_min);
                    right->delete_key(key_min);
                    this->keys[index] = key_min;
                }
                // merge
                else{
                    while(right->size > 0){
                        Key min_key = right->data_pointers.begin()->first;
                        RecordPtr min_rec = right->data_pointers.begin()->second;
                        leaf->insert_key(min_key,min_rec);
                        right->delete_key(min_key);
                    }
                    
                    leaf->next_leaf_ptr = right->next_leaf_ptr;
                    this->keys.erase(this->keys.begin() + (index));
                    this->tree_pointers.erase(this->tree_pointers.begin() + index + 1);
                    this->size = this->tree_pointers.size();
                }
                leaf->dump();
                // delete right1;
                delete right;
                // delete leaf;
            }
        }
        else if(curr_node->node_type==INTERNAL){
            InternalNode* internal = (InternalNode*)curr_node;
            // left-sibling
            if(index>0){
                InternalNode* left = (InternalNode*)TreeNode::tree_node_factory(this->tree_pointers[index-1]);
                int size_of_left = left->size;
                // re-distribution
                if(size_of_left>MIN_OCCUPANCY){
                    Key root_key = this->keys[index-1];
                    Key key_last = left->keys.back();
                    left->keys.pop_back();
                    TreePtr treeptr_last = left->tree_pointers.back();
                    left->tree_pointers.pop_back();
                    left->size--;
                    internal->keys.insert(internal->keys.begin(),root_key);
                    internal->tree_pointers.insert(internal->tree_pointers.begin(),treeptr_last);
                    this->keys[index-1]=key_last;
                    left->dump();
                    internal->dump();
                    delete left;
                    // delete internal;
                }
                // merge
                else{
                    int child_size = internal->size;
                    Key key=DELETE_MARKER;
                    while(true){
                        if(child_size==0)
                            break;
                        if(child_size-1 > 0){
                            key = internal->keys[0];
                            internal->keys.erase(left->keys.begin());
                        }
                        TreePtr treeptr = internal->tree_pointers[0];
                        internal->tree_pointers.erase(internal->tree_pointers.begin());
                        internal->size--;
                        Key root = this->keys[index-1];
                        left->keys.push_back(root);
                        left->tree_pointers.push_back(treeptr);
                        left->size++;
                        if(child_size-1 != 0)
                            this->keys[index-1]=key;
                        child_size = internal->size;
                    }
                    this->keys.erase(this->keys.begin() + index- 1);
                    this->tree_pointers.erase(this->tree_pointers.begin() + index);
                    this->size--;
                    left->dump();
                }
                delete left;
                delete internal;
            }
            // right-sibling
            else{
                InternalNode* right = (InternalNode*)TreeNode::tree_node_factory(this->tree_pointers[index+1]);
                int size_of_right = right->size;
                // Re-distribution
                if(size_of_right > MIN_OCCUPANCY){
                    Key root = this->keys[index];
                    Key key_new = right->keys[0];
                    right->keys.erase(right->keys.begin());
                    TreePtr treeptr_new = right->tree_pointers[0];
                    right->tree_pointers.erase(right->tree_pointers.begin());
                    right->size--;

                    this->keys[index] = key_new;

                    internal->keys.push_back(root);
                    internal->tree_pointers.push_back(treeptr_new);
                    internal->size++;

                    internal->dump();
                    right->dump();
                }
                // merge
                else{
                    int child_size = internal->size;
                    Key key=DELETE_MARKER;
                    while(true){
                        if(child_size==0)
                            break;
                        Key root = this->keys[index];
                        if(child_size-1 > 0){
                            key = internal->keys.back();
                            internal->keys.pop_back();
                        }
                        TreePtr ptr = internal->tree_pointers.back();
                        internal->tree_pointers.pop_back();
                        internal->size--;
                        right->keys.insert(right->keys.begin(),root);
                        right->tree_pointers.insert(right->tree_pointers.begin(),ptr);
                        right->size++;
                        child_size = internal->size;
                        if(child_size-1!=0)
                            this->keys[index]=key;
                    }
                    this->keys.erase(this->keys.begin() + index);
                    this->tree_pointers.erase(this->tree_pointers.begin() + index);
                    this->size--;

                    right->dump();
                }
                delete internal;
                delete right;
            }
        }
    }

    this->dump();
}

//runs range query on subtree rooted at this node
void InternalNode::range(ostream &os, const Key &min_key, const Key &max_key) const {
    BLOCK_ACCESSES++;
    for (int i = 0; i < this->size - 1; i++) {
        if (min_key <= this->keys[i]) {
            auto* child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
            child_node->range(os, min_key, max_key);
            delete child_node;
            return;
        }
    }
    auto* child_node = TreeNode::tree_node_factory(this->tree_pointers[this->size - 1]);
    child_node->range(os, min_key, max_key);
    delete child_node;
}

//exports node - used for grading
void InternalNode::export_node(ostream &os) {
    TreeNode::export_node(os);
    for (int i = 0; i < this->size - 1; i++)
        os << this->keys[i] << " ";
    os << endl;
    for (int i = 0; i < this->size; i++) {
        auto child_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        child_node->export_node(os);
        delete child_node;
    }
}

//writes subtree rooted at this node as a mermaid chart
void InternalNode::chart(ostream &os) {
    string chart_node = this->tree_ptr + "[" + this->tree_ptr + BREAK;
    chart_node += "size: " + to_string(this->size) + BREAK;
    chart_node += "]";
    os << chart_node << endl;

    for (int i = 0; i < this->size; i++) {
        auto tree_node = TreeNode::tree_node_factory(this->tree_pointers[i]);
        tree_node->chart(os);
        delete tree_node;
        string link = this->tree_ptr + "-->|";

        if (i == 0)
            link += "x <= " + to_string(this->keys[i]);
        else if (i == this->size - 1) {
            link += to_string(this->keys[i - 1]) + " < x";
        } else {
            link += to_string(this->keys[i - 1]) + " < x <= " + to_string(this->keys[i]);
        }
        link += "|" + this->tree_pointers[i];
        os << link << endl;
    }
}

ostream& InternalNode::write(ostream &os) const {
    TreeNode::write(os);
    for (int i = 0; i < this->size - 1; i++) {
        if (&os == &cout)
            os << "\nP" << i + 1 << ": ";
        os << this->tree_pointers[i] << " ";
        if (&os == &cout)
            os << "\nK" << i + 1 << ": ";
        os << this->keys[i] << " ";
    }
    if (&os == &cout)
        os << "\nP" << this->size << ": ";
    os << this->tree_pointers[this->size - 1];
    return os;
}

istream& InternalNode::read(istream& is) {
    TreeNode::read(is);
    this->keys.assign(this->size - 1, DELETE_MARKER);
    this->tree_pointers.assign(this->size, NULL_PTR);
    for (int i = 0; i < this->size - 1; i++) {
        if (&is == &cin)
            cout << "P" << i + 1 << ": ";
        is >> this->tree_pointers[i];
        if (&is == &cin)
            cout << "K" << i + 1 << ": ";
        is >> this->keys[i];
    }
    if (&is == &cin)
        cout << "P" << this->size;
    is >> this->tree_pointers[this->size - 1];
    return is;
}
